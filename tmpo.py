__title__ = "tmpo"
__version__ = "0.2.0"
__build__ = 0x000100
__author__ = "Bart Van Der Meerssche"
__license__ = "MIT"
__copyright__ = "Copyright 2014 Bart Van Der Meerssche"

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
    SELECT rid, lvl, bid
    FROM tmpo
    WHERE sid = ?
    ORDER BY created DESC, lvl DESC
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


class Session():
    def __init__(self, path=None, workers=16):
        self.debug = False
        if path is None:
            path = os.environ["HOME"]
        if _platform == "win32":
            self.home = path + os.sep + "tmpo"
        elif:
            self.home = path + os.sep + ".tmpo"
        self.db = self.home + os.sep + "tmpo.sqlite3"
        self.crt = self.home + os.sep + "flukso.crt"
        self.host = "api.flukso.net"
        try:
            os.mkdir(self.home)
        except OSError:  # dir exists
            pass
        else:
            with io.open(self.crt, "wb") as f:
                f.write(FLUKSO_CRT.encode("ascii"))
        self.dbcon = sqlite3.connect(self.db)
        self.dbcur = self.dbcon.cursor()
        self.dbcur.execute(SQL_SENSOR_TABLE)
        self.dbcur.execute(SQL_TMPO_TABLE)
        self.dbcon.commit()
        self.rqs = requests_futures.sessions.FuturesSession(
            executor=concurrent.futures.ThreadPoolExecutor(
                max_workers=workers))
        self.rqs.headers.update({"X-Version": "1.0"})

    def add(self, sid, token):
        try:
            self.dbcur.execute(SQL_SENSOR_INS, (sid, token))
            self.dbcon.commit()
        except sqlite3.IntegrityError:  # sensor entry exists
            pass

    def remove(self, sid):
        self.dbcur.execute(SQL_SENSOR_DEL, (sid,))
        self.dbcon.commit()

    def sync(self, *sids):
        if sids == ():
            sids = [sid for (sid,) in self.dbcur.execute(SQL_SENSOR_ALL)]
        for sid in sids:
            self.dbcur.execute(SQL_TMPO_LAST, (sid,))
            last = self.dbcur.fetchone()
            if last:
                rid, lvl, bid = last
                self._clean(sid, rid, lvl, bid)
                # prevent needless polling
                if time.time() < bid + 256:
                    return
            else:
                rid, lvl, bid = 0, 0, 0
            self._req_sync(sid, rid, lvl, bid)

    def list(self, *sids):
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

    def series(self, sid, recycle_id=None, head=0, tail=EPOCHS_MAX,
               datetime=True):
        head = self._2epochs(head)
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
            ts = pd.concat(srlist).truncate(before=head, after=tail)
            ts.name = sid
            if datetime is True:
                ts.index = pd.to_datetime(ts.index, unit="s", utc=True)
            return ts
        else:
            return pd.Series([], name=sid)

    def dataframe(self, sids, head=0, tail=EPOCHS_MAX, datetime=True):
        head = self._2epochs(head)
        tail = self._2epochs(tail)
        series = [self.series(sid, head=head, tail=tail, datetime=False)
                  for sid in sids]
        df = pd.concat(series, axis=1)
        if datetime is True:
            df.index = pd.to_datetime(df.index, unit="s", utc=True)
        return df

    def _2epochs(self, time):
        if isinstance(time, pd.tslib.Timestamp):
            return int(math.floor(time.value / 1e9))
        elif isinstance(time, int):
            return time
        else:
            raise TmpoError("Time format not supported. " +
                            "Use epochs or a Pandas timestamp.")

    def _blk2series(self, ext, blk, head, tail):
        if ext != "gz":
            raise NotImplementedError("Compression type not supported in tmpo")
        jblk = zlib.decompress(blk, zlib.MAX_WBITS | 16)  # gzip decoding
        m = re.match(RE_JSON_BLK, jblk.decode("utf-8"))
        pdjblk = '{"index":%s,"data":%s}' % (m.group("t"), m.group("v"))
        pdsblk = pd.read_json(
            pdjblk,
            typ="series",
            dtype="float",
            orient="split",
            numpy=True,
            date_unit="s")
        h = json.loads(m.group("h"))
        self._npdelta(pdsblk.index, h["head"][0])
        self._npdelta(pdsblk, h["head"][1])
        return pdsblk

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
        self.dbcon.commit()
        self._clean(sid, rid, lvl, bid)
        self._dprintf(DBG_TMPO_WRITE, now, sid, rid, lvl, bid, len(blk))

    def _clean(self, sid, rid, lvl, bid):
        if lvl == 8:
            return
        lastchild = self._lastchild(lvl, bid)
        self.dbcur.execute(SQL_TMPO_CLEAN, (sid, rid, lvl - 4, lastchild))
        self.dbcon.commit()
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
