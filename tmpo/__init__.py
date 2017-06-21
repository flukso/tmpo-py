__title__ = "tmpo"
__version__ = "0.2.7"
__build__ = 0x000100
__author__ = "Bart Van Der Meerssche"
__license__ = "MIT"
__copyright__ = "Copyright 2017 Bart Van Der Meerssche"

FLUKSO_CRT = """
-----BEGIN CERTIFICATE-----
MIIDfzCCAmegAwIBAgIJANYOkpI6yVcFMA0GCSqGSIb3DQEBBQUAMDMxCzAJBgNV
BAYTAkJFMQ8wDQYDVQQKEwZGbHVrc28xEzARBgNVBAMTCmZsdWtzby5uZXQwHhcN
MTAwNjAxMjE1ODAyWhcNMzUwNTI2MjE1ODAyWjAzMQswCQYDVQQGEwJCRTEPMA0G
A1UEChMGRmx1a3NvMRMwEQYDVQQDEwpmbHVrc28ubmV0MIIBIjANBgkqhkiG9w0B
AQEFAAOCAQ8AMIIBCgKCAQEA6CtNI3YrF/7Ak3etIe+XnL4HwJYki4PyaWI4S7W1
49C9W5AEbEd7ufnsaku3eVxMqOP6b5L7MFpCCGDiM1Zt32yYAcL65eCrofZw1DE0
SuWos0Z1P4y2rIUFHya8g8bUh7lUvq30IBgnnUh7Lo0eQT1XfnC/KMUnvseHI/iw
Y3HhYX+espsCPh1a0ATLlEk93XK99q/5mgojSGQxmwPj/91mOWmJOO4edEQAhK+u
t6wCNxZNnf9yyyzzLczwMytfrwBWJEJjJFTfr3JiEmHdl4dt7UiuElGLMr9dFhPV
12Bidxszov663ffUiIUmV/fkMWF1ZEWXFS0x+VJ52seChwIDAQABo4GVMIGSMB0G
A1UdDgQWBBQGMvERFrapN1lmOm9SVR8qB+uj/zBjBgNVHSMEXDBagBQGMvERFrap
N1lmOm9SVR8qB+uj/6E3pDUwMzELMAkGA1UEBhMCQkUxDzANBgNVBAoTBkZsdWtz
bzETMBEGA1UEAxMKZmx1a3NvLm5ldIIJANYOkpI6yVcFMAwGA1UdEwQFMAMBAf8w
DQYJKoZIhvcNAQEFBQADggEBAOZjgNoNhJLckVMEYZiYWqRDWeRPBkyGStCH93r3
42PpuKDyysxI1ldLTcUpUSrs1AtdSIEiEahWr6zVW4QW4o9iqO905E03aTO86L+P
j7SIBPP01M2f70pHpnz+uH1MDxsarI96qllslWfymYI7c6yUN/VciWfNWa38nK1l
MiQJuDvElNy8aN1JJtXHFUQK/I8ois1ATT1rGAiqrkDZIm4pdDmqB/zLI3qIJf8o
cKIo2x/YkVhuDmIpU/XVA13csXrXU+CLfFyNdY1a/6Dhv2B4wG6J5RGuxWmA+Igg
TTysD+aqqzs8XstqDu/aLjMzFKMaXNvDoCbdFQGVXfx0F1A=
-----END CERTIFICATE-----"""

SQL_SENSOR_TABLE = """
    CREATE TABLE IF NOT EXISTS sensor(
    sid TEXT,
    token TEXT,
    PRIMARY KEY(sid))"""

SQL_SENSOR_INS = """
    INSERT INTO sensor
    (sid, token)
    VALUES (?, ?)"""

SQL_SENSOR_DEL = """
    DELETE FROM sensor
    WHERE sid = ?"""

SQL_SENSOR_ALL = """
    SELECT sid
    FROM sensor
    ORDER BY sid"""

SQL_SENSOR_TOKEN = """
    SELECT token
    FROM sensor
    WHERE sid = ?"""

SQL_TMPO_TABLE = """
    CREATE TABLE IF NOT EXISTS tmpo(
    sid TEXT,
    rid INTEGER,
    lvl INTEGER,
    bid INTEGER,
    ext TEXT,
    created REAL,
    data BLOB,
    PRIMARY KEY(sid, rid, lvl, bid))"""

SQL_TMPO_INS = """
    INSERT INTO tmpo
    (sid, rid, lvl, bid, ext, created, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)"""

SQL_TMPO_CLEAN = """
    DELETE
    FROM tmpo
    WHERE sid = ? AND rid = ? AND lvl = ? AND bid <= ?"""

SQL_TMPO_ALL = """
    SELECT sid, rid, lvl, bid, ext, created, data
    FROM tmpo
    WHERE sid = ?
    ORDER BY rid ASC, lvl DESC, bid ASC"""

SQL_TMPO_LAST = """
    SELECT rid, lvl, bid, ext, data
    FROM tmpo
    WHERE sid = ?
    ORDER BY created DESC, lvl DESC
    LIMIT 1"""

SQL_TMPO_FIRST = """
    SELECT rid, lvl, bid
    FROM tmpo
    WHERE sid = ?
    ORDER BY created ASC, lvl ASC
    LIMIT 1"""

SQL_TMPO_RID_MAX = """
    SELECT MAX(rid)
    FROM tmpo
    WHERE sid = ?"""

API_TMPO_SYNC = "https://%s/sensor/%s/tmpo/sync"
API_TMPO_BLOCK = "https://%s/sensor/%s/tmpo/%d/%d/%d"

HTTP_ACCEPT = {
    "json": "application/json",
    "gz": "application/gzip"}

RE_JSON_BLK = r'^\{"h":(?P<h>\{.+?\}),"t":(?P<t>\[.+?\]),"v":(?P<v>\[.+?\])\}$'
DBG_TMPO_REQUEST = "[r] time:%.3f sid:%s rid:%d lvl:%2d bid:%d"
DBG_TMPO_WRITE = "[w] time:%.3f sid:%s rid:%d lvl:%2d bid:%d size[B]:%d"
EPOCHS_MAX = 2147483647


import os
import sys
import io
import math
import time
import sqlite3
import requests_futures.sessions
import concurrent.futures
import zlib
import re
import json
import numpy as np
import pandas as pd
from functools import wraps


def dbcon(func):
    """Set up connection before executing function, commit and close connection
    afterwards. Unless a connection already has been created."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if self.dbcon is None:
            # set up connection
            self.dbcon = sqlite3.connect(self.db)
            self.dbcur = self.dbcon.cursor()
            self.dbcur.execute(SQL_SENSOR_TABLE)
            self.dbcur.execute(SQL_TMPO_TABLE)

            # execute function
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                # on exception, first close connection and then raise
                self.dbcon.rollback()
                self.dbcon.commit()
                self.dbcon.close()
                self.dbcon = None
                self.dbcur = None
                raise e
            else:
                # commit everything and close connection
                self.dbcon.commit()
                self.dbcon.close()
                self.dbcon = None
                self.dbcur = None
        else:
            result = func(*args, **kwargs)
        return result
    return wrapper


class Session():
    def __init__(self, path=None, workers=16):
        """
        Parameters
        ----------
        path : str, optional
            location for the database
        workers : int
            default 16
        """
        self.debug = False
        if path is None:
            path = os.path.expanduser("~") # $HOME
        if sys.platform == "win32":
            self.home = os.path.join(path, "tmpo")
        else:
            self.home = os.path.join(path, ".tmpo")
        self.db = os.path.join(self.home, "tmpo.sqlite3")
        self.crt = os.path.join(self.home, "flukso.crt")
        self.host = "api.flukso.net"
        try:
            os.mkdir(self.home)
        except OSError:  # dir exists
            pass
        else:
            with io.open(self.crt, "wb") as f:
                f.write(FLUKSO_CRT.encode("ascii"))
        self.rqs = requests_futures.sessions.FuturesSession(
            executor=concurrent.futures.ThreadPoolExecutor(
                max_workers=workers))
        self.rqs.headers.update({"X-Version": "1.0"})
        self.dbcon = None
        self.dbcur = None

    @dbcon
    def add(self, sid, token):
        """
        Add new sensor to the database

        Parameters
        ----------
        sid : str
            SensorId
        token : str
        """
        try:
            self.dbcur.execute(SQL_SENSOR_INS, (sid, token))
        except sqlite3.IntegrityError:  # sensor entry exists
            pass

    @dbcon
    def remove(self, sid):
        """
        Remove sensor from the database

        Parameters
        ----------
        sid : str
            SensorID
        """
        self.dbcur.execute(SQL_SENSOR_DEL, (sid,))

    @dbcon
    def sync(self, *sids):
        """
        Synchronise data

        Parameters
        ----------
        sids : list of str
            SensorIDs to sync
            Optional, leave empty to sync everything
        """
        if sids == ():
            sids = [sid for (sid,) in self.dbcur.execute(SQL_SENSOR_ALL)]
        for sid in sids:
            self.dbcur.execute(SQL_TMPO_LAST, (sid,))
            last = self.dbcur.fetchone()
            if last:
                rid, lvl, bid, ext, blk = last
                self._clean(sid, rid, lvl, bid)
                # prevent needless polling
                if time.time() < bid + 256:
                    return
            else:
                rid, lvl, bid = 0, 0, 0
            self._req_sync(sid, rid, lvl, bid)

    @dbcon
    def list(self, *sids):
        """
        List all tmpo-blocks in the database

        Parameters
        ----------
        sids : list of str
            SensorID's for which to list blocks
            Optional, leave empty to get them all

        Returns
        -------
        list[list[tuple]]
        """
        if sids == ():
            sids = [sid for (sid,) in self.dbcur.execute(SQL_SENSOR_ALL)]
        slist = []
        for sid in sids:
            tlist = []
            for tmpo in self.dbcur.execute(SQL_TMPO_ALL, (sid,)):
                tlist.append(tmpo)
                sid, rid, lvl, bid, ext, ctd, blk = tmpo
                self._dprintf(DBG_TMPO_WRITE, ctd, sid, rid, lvl, bid, len(blk))
            slist.append(tlist)
        return slist

    @dbcon
    def series(self, sid, recycle_id=None, head=None, tail=None,
               datetime=True):
        """
        Create data Series

        Parameters
        ----------
        sid : str
        recycle_id : optional
        head : int | pandas.tslib.Timestamp, optional
            Start of the interval
            default earliest available
        tail : int | pandas.tslib.Timestamp, optional
            End of the interval
            default max epoch
        datetime : bool
            convert index to datetime
            default True

        Returns
        -------
        pandas.Series
        """
        if head is None:
            head = 0
        else:
            head = self._2epochs(head)

        if tail is None:
            tail = EPOCHS_MAX
        else:
            tail = self._2epochs(tail)

        if recycle_id is None:
            self.dbcur.execute(SQL_TMPO_RID_MAX, (sid,))
            recycle_id = self.dbcur.fetchone()[0]
        tlist = self.list(sid)[0]
        srlist = []
        for _sid, rid, lvl, bid, ext, ctd, blk in tlist:
            if (recycle_id == rid
            and head < self._blocktail(lvl, bid)
            and tail >= bid):
                srlist.append(self._blk2series(ext, blk, head, tail))
        if len(srlist) > 0:
            ts = pd.concat(srlist)
            ts.name = sid
            if datetime is True:
                ts.index = pd.to_datetime(ts.index, unit="s", utc=True)
            return ts
        else:
            return pd.Series([], name=sid)

    @dbcon
    def dataframe(self, sids, head=0, tail=EPOCHS_MAX, datetime=True):
        """
        Create data frame

        Parameters
        ----------
        sids : list[str]
        head : int | pandas.tslib.Timestamp, optional
            Start of the interval
            default earliest available
        tail : int | pandas.tslib.Timestamp, optional
            End of the interval
            default max epoch
        datetime : bool
            convert index to datetime
            default True

        Returns
        -------
        pandas.DataFrame
        """
        if head is None:
            head = 0
        else:
            head = self._2epochs(head)

        if tail is None:
            tail = EPOCHS_MAX
        else:
            tail = self._2epochs(tail)

        series = [self.series(sid, head=head, tail=tail, datetime=False)
                  for sid in sids]
        df = pd.concat(series, axis=1)
        if datetime is True:
            df.index = pd.to_datetime(df.index, unit="s", utc=True)
        return df

    @dbcon
    def first_timestamp(self, sid, epoch=False):
        """
        Get the first available timestamp for a sensor

        Parameters
        ----------
        sid : str
            SensorID
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        pd.Timestamp | int
        """
        first_block = self.dbcur.execute(SQL_TMPO_FIRST, (sid,)).fetchone()
        if first_block is None:
            return None

        timestamp = first_block[2]
        if epoch:
            return timestamp
        else:
            return pd.Timestamp.fromtimestamp(timestamp).tz_localize('UTC')

    def last_timestamp(self, sid, epoch=False):
        """
        Get the theoretical last timestamp for a sensor

        Parameters
        ----------
        sid : str
            SensorID
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        pd.Timestamp | int
        """
        timestamp, value = self.last_datapoint(sid, epoch)
        return timestamp

    def last_datapoint(self, sid, epoch=False):
        """
        Parameters
        ----------
        sid : str
            SensorId
        epoch : bool
            default False
            If True return as epoch
            If False return as pd.Timestamp

        Returns
        -------
        pd.Timestamp | int, float
        """
        block = self._last_block(sid)
        if block is None:
            return None, None

        header = block['h']
        timestamp, value = header['tail']

        if not epoch:
            timestamp = pd.Timestamp.fromtimestamp(timestamp).tz_localize('UTC')

        return timestamp, value

    @dbcon
    def _last_block(self, sid):
        cur = self.dbcur.execute(SQL_TMPO_LAST, (sid,))
        row = cur.fetchone()
        if row is None:
            return None

        rid, lvl, bid, ext, blk = row

        jblk = self._decompress_block(blk, ext)
        data = json.loads(jblk.decode('UTF-8'))
        return data

    def _decompress_block(self, blk, ext):
        if ext != "gz":
            raise NotImplementedError("Compression type not supported in tmpo")
        jblk = zlib.decompress(blk, zlib.MAX_WBITS | 16)  # gzip decoding
        return jblk

    def _2epochs(self, time):
        if isinstance(time, pd.tslib.Timestamp):
            return int(math.floor(time.value / 1e9))
        elif isinstance(time, int):
            return time
        else:
            raise NotImplementedError("Time format not supported. " +
                                      "Use epochs or a Pandas timestamp.")

    def _blk2series(self, ext, blk, head, tail):
        jblk = self._decompress_block(blk, ext)
        m = re.match(RE_JSON_BLK, jblk.decode("utf-8"))
        pdjblk = '{"index":%s,"data":%s}' % (m.group("t"), m.group("v"))
        try:
            pdsblk = pd.read_json(
                pdjblk,
                typ="series",
                dtype="float",
                orient="split",
                numpy=True,
                date_unit="s")
        except:
            return pd.Series()
        h = json.loads(m.group("h"))
        self._npdelta(pdsblk.index, h["head"][0])
        self._npdelta(pdsblk, h["head"][1])
        # Use the built-in ix method to truncate
        pdsblk_truncated = pdsblk.ix[head:tail]
        return pdsblk_truncated

    def _npdelta(self, a, delta):
        """Numpy: Modifying Array Values
            http://docs.scipy.org/doc/numpy/reference/arrays.nditer.html"""
        for x in np.nditer(a, op_flags=["readwrite"]):
            delta += x
            x[...] = delta
        return a

    def _req_sync(self, sid, rid, lvl, bid):
        self.dbcur.execute(SQL_SENSOR_TOKEN, (sid,))
        token, = self.dbcur.fetchone()
        headers = {
            "Accept": HTTP_ACCEPT["json"],
            "X-Token": token}
        params = {
            "rid": rid,
            "lvl": lvl,
            "bid": bid}
        f = self.rqs.get(
            API_TMPO_SYNC % (self.host, sid),
            headers=headers,
            params=params,
            verify=self.crt)
        r = f.result()
        r.raise_for_status()
        fs = []
        for t in r.json():
            fs.append((t, self._req_block(
                sid, token, t["rid"], t["lvl"], t["bid"], t["ext"])))
        for (t, f) in fs:
            self._write_block(
                f.result(), sid, t["rid"], t["lvl"], t["bid"], t["ext"])

    def _req_block(self, sid, token, rid, lvl, bid, ext):
        headers = {
            "Accept": HTTP_ACCEPT["gz"],
            "X-Token": token}
        f = self.rqs.get(
            API_TMPO_BLOCK % (self.host, sid, rid, lvl, bid),
            headers=headers,
            verify=self.crt)
        self._dprintf(DBG_TMPO_REQUEST, time.time(), sid, rid, lvl, bid)
        return f

    def _write_block(self, r, sid, rid, lvl, bid, ext):
        blk = sqlite3.Binary(r.content)
        now = time.time()
        self.dbcur.execute(SQL_TMPO_INS, (sid, rid, lvl, bid, ext, now, blk))
        self._clean(sid, rid, lvl, bid)
        self._dprintf(DBG_TMPO_WRITE, now, sid, rid, lvl, bid, len(blk))

    def _clean(self, sid, rid, lvl, bid):
        if lvl == 8:
            return
        lastchild = self._lastchild(lvl, bid)
        self.dbcur.execute(SQL_TMPO_CLEAN, (sid, rid, lvl - 4, lastchild))
        self._clean(sid, rid, lvl - 4, lastchild)

    def _lastchild(self, lvl, bid):
        delta = math.trunc(2 ** (lvl - 4))
        return bid + 15 * delta

    def _blocktail(self, lvl, bid):
        delta = math.trunc(2 ** lvl)
        return bid + delta

    def _dprintf(self, fmt, *args):
        if self.debug:
            print(fmt % args)
