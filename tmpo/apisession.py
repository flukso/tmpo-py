import os
import requests_futures.sessions
import concurrent.futures
import time
import math
import pandas as pd
import re
import zlib
import json
import numpy as np

HTTP_ACCEPT = {
    "json": "application/json",
    "gz": "application/gzip"}

API_TMPO_SYNC = "https://%s/sensor/%s/tmpo/sync"
API_TMPO_BLOCK = "https://%s/sensor/%s/tmpo/%d/%d/%d"

DBG_TMPO_REQUEST = "[r] time:%.3f sid:%s rid:%d lvl:%2d bid:%d"
RE_JSON_BLK = r'^\{"h":(?P<h>\{.+?\}),"t":(?P<t>\[.+?\]),"v":(?P<v>\[.+?\])\}$'
EPOCHS_MAX = 2147483647


class APISession:
    """
    Get Flukso Data (aka. TMPO Blocks) directly from the Flukso API
    """
    def __init__(self, workers=16, cert=None):
        """
        Parameters
        ----------
        workers : int
            default 16
        """
        self.debug = False

        package_dir = os.path.dirname(__file__)
        if cert != False:
            self.crt = os.path.join(package_dir, ".flukso.crt")
        else:
            self.crt = cert
        self.host = "api.flukso.net"

        self.rqs = requests_futures.sessions.FuturesSession(
            executor=concurrent.futures.ThreadPoolExecutor(
                max_workers=workers))
        self.rqs.headers.update({"X-Version": "1.0"})

        self.sensors = {}

    def add(self, sid, token):
        """
        Add a sensor and token to the client, so you don't have to enter the
        token for each call (also for compatibility with sqlitesession)

        Parameters
        ----------
        sid : str
        token : str
        """
        self.sensors.update({sid: token})

    def series(self, sid, recycle_id=None, head=None, tail=None,
               datetime=True, token=None):
        """
        Create data Series

        Parameters
        ----------
        sid : str
        token : str, optional
        recycle_id : optional
        head : int | pandas.Timestamp, optional
            Start of the interval
            default earliest available
        tail : int | pandas.Timestamp, optional
            End of the interval
            default max epoch
        datetime : bool
            convert index to datetime
            default True

        Returns
        -------
        pd.Series
        """
        token = token if token else self.sensors[sid]

        if head is None:
            head = 0
        else:
            head = self._2epochs(head)

        if tail is None:
            tail = EPOCHS_MAX
        else:
            tail = self._2epochs(tail)

        blist = self._req_blocklist(sid=sid, token=token, rid=recycle_id)
        blist = self._slice_blist(blist=blist, head=head, tail=tail)
        if len(blist) == 0:
            return pd.Series([], name=sid)
        blocks_futures = self._req_blocks(sid=sid, token=token, blist=blist)

        def parse_blocks():
            for h, future in blocks_futures:
                r = future.result()
                r.raise_for_status()
                _ts = self._blk2series(ext=h['ext'], blk=r.content,
                                       head=head, tail=tail)
                yield _ts

        blocks = parse_blocks()
        ts = pd.concat(blocks)
        if ts.empty:
            return pd.Series([], name=sid)
        ts.name = sid
        if datetime is True:
            ts.index = pd.to_datetime(ts.index, unit='s', utc=True)
        return ts

    def dataframe(self, sids, head=0, tail=EPOCHS_MAX, datetime=True):
        """
        Create data frame

        Parameters
        ----------
        sids : [str]
            List of SensorID's
        head : int | pandas.Timestamp, optional
            Start of the interval
            default earliest available
        tail : int | pandas.Timestamp, optional
            End of the interval
            default max epoch
        datetime : bool
            convert index to datetime
            default True

        Returns
        -------
        pd.DataFrame
        """
        series = (self.series(sid, head=head, tail=tail, datetime=False)
                  for sid in sids)
        df = pd.concat(series, axis=1)
        if datetime is True:
            df.index = pd.to_datetime(df.index, unit="s", utc=True)
        return df

    def first_timestamp(self, sid, token=None, epoch=False):
        """
        Get the first available timestamp for a sensor

        Parameters
        ----------
        sid : str
            SensorID
        token : str, optional
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        pd.Timestamp | int
        """
        token = token if token else self.sensors[sid]

        blist = self._req_blocklist(sid=sid, token=token)
        if len(blist) == 0:
            return None
        first = blist[0]['bid']
        if not epoch:
            first = self._epoch2timestamp(first)
        return first

    def last_timestamp(self, sid, token=None, epoch=False):
        """
        Get the last timestamp for a sensor

        Parameters
        ----------
        sid : str
            SensorID
        token: str, optional
        token : str
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        pd.Timestamp | int
        """
        last, _v = self.last_datapoint(sid=sid, token=token, epoch=epoch)
        return last

    def last_datapoint(self, sid, token=None, epoch=False):
        """
        Parameters
        ----------
        sid : str
            SensorId
        token : str, optional
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        (pd.Timestamp, float) or (int, float)
        """
        block = self._last_block(sid=sid, token=token)
        if block is None:
            return None, None

        header = block['h']
        timestamp, value = header['tail']

        if not epoch:
            timestamp = self._epoch2timestamp(timestamp)

        return timestamp, value

    def _last_block(self, sid, token=None):
        token = token if token else self.sensors[sid]

        blist = self._req_blocklist(sid=sid, token=token)
        last = blist[-1]
        bf = self._req_block(sid=sid, token=token, rid=last['rid'],
                             lvl=last['lvl'], bid=last['bid'], ext=last['ext'])
        r = bf.result()
        r.raise_for_status()

        jblk = self._decompress_block(blk=r.content, ext=last['ext'])
        data = json.loads(jblk.decode('UTF-8'))
        return data

    def _epoch2timestamp(self, epoch):
        timestamp = pd.Timestamp.utcfromtimestamp(epoch)
        timestamp = timestamp.tz_localize('UTC')
        return timestamp

    def _2epochs(self, time):
        if isinstance(time, pd.Timestamp):
            return int(math.floor(time.value / 1e9))
        elif isinstance(time, int):
            return time
        else:
            raise NotImplementedError("Time format not supported. " +
                                      "Use epochs or a Pandas timestamp.")

    def _blk2series(self, ext, blk, head, tail):
        jblk = self._decompress_block(blk, ext)
        m = re.match(RE_JSON_BLK, jblk.decode("utf-8"))
        if m is None:
            return pd.Series()
        pdjblk = '{"index":%s,"data":%s}' % (m.group("t"), m.group("v"))
        try:
            pdsblk = pd.read_json(
                pdjblk,
                typ="series",
                dtype="float",
                orient="split",
                date_unit="s")
        except:
            return pd.Series()
        h = json.loads(m.group("h"))
        self._npdelta(pdsblk.index, h["head"][0])
        self._npdelta(pdsblk, h["head"][1])
        pdsblk_truncated = pdsblk.loc[head:tail]
        return pdsblk_truncated

    def _decompress_block(self, blk, ext):
        if ext != "gz":
            raise NotImplementedError("Compression type not supported in tmpo")
        jblk = zlib.decompress(blk, zlib.MAX_WBITS | 16)  # gzip decoding
        return jblk

    def _req_blocklist(self, sid, token, rid=0, lvl=0, bid=0):
        """
        Request the list of available blocks from the API

        Parameters
        ----------
        sid : str
        token : str
        rid : int
        lvl : int
        bid : int

        Returns
        -------
        [dict]
        """
        headers = {
            "Accept": HTTP_ACCEPT["json"],
            "X-Token": token}
        params = {
            "rid": rid if rid else 0,
            "lvl": lvl if rid else 0,
            "bid": bid if rid else 0}
        f = self.rqs.get(
            API_TMPO_SYNC % ("api.flukso.net", sid),
            headers=headers,
            params=params,
            verify=self.crt
        )
        r = f.result()
        r.raise_for_status()
        j = r.json()
        blist = sorted(j, key=lambda x: x['bid'])
        return blist

    def _req_block(self, sid, token, rid, lvl, bid, ext):
        """
        Request a block (as a request future) from the API

        Parameters
        ----------
        sid : str
        token : str
        rid : int
        lvl : int
        bid : int
        ext : str

        Returns
        -------
        Response
        """
        headers = {
            "Accept": HTTP_ACCEPT["gz"],
            "X-Token": token}
        f = self.rqs.get(
            API_TMPO_BLOCK % (self.host, sid, rid, lvl, bid),
            headers=headers,
            verify=self.crt)
        self._dprintf(DBG_TMPO_REQUEST, time.time(), sid, rid, lvl, bid)
        return f

    def _req_blocks(self, sid, token, blist):
        """
        Request multiple blocks (as a list of request futures)

        Parameters
        ----------
        sid : str
        token : str
        blist : [dict]
            [
                {'rid': 0, 'lvl': 20, 'bid': 1506803712, 'ext': 'gz'},
                ...
            ]

        Returns
        -------
        (dict, Response)
        """
        fs = []
        for t in blist:
            fs.append(
                (t, self._req_block(sid=sid, token=token, rid=t['rid'],
                                    lvl=t['lvl'], bid=t['bid'], ext=t['ext'])))
        return fs

    @staticmethod
    def _npdelta(a, delta):
        """Numpy: Modifying Array Values
            http://docs.scipy.org/doc/numpy/reference/arrays.nditer.html"""
        for x in np.nditer(a, op_flags=["readwrite"]):
            delta += x
            x[...] = delta
        return a

    def _lastchild(self, lvl, bid):
        delta = math.trunc(2 ** (lvl - 4))
        return bid + 15 * delta

    def _blocktail(self, lvl, bid):
        delta = math.trunc(2 ** lvl)
        return bid + delta

    def _dprintf(self, fmt, *args):
        if self.debug:
            print(fmt % args)

    def _slice_blist(self, blist, head, tail):
        """
        Slice a blist (block headers) so it contains all blocks with data
        between head and tail

        Parameters
        ----------
        blist : [dict]
            [
                {'rid': 0, 'lvl': 20, 'bid': 1506803712, 'ext': 'gz'},
                ...
            ]
        head : int
            epochs
        tail : int
            epochs

        Returns
        -------
        [dict]
        """
        ret = []
        for b in blist:
            if head < self._blocktail(lvl=b['lvl'], bid=b['bid']) and tail >= b['bid']:
                ret.append(b)
        return ret
