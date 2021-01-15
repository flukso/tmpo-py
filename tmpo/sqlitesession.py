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

SQL_TMPO_DEL = """
    DELETE FROM tmpo
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
    SELECT rid, lvl, bid, ext
    FROM tmpo
    WHERE sid = ?
    ORDER BY created DESC, lvl DESC
    LIMIT 1"""

SQL_TMPO_LAST_DATA = """
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

DBG_TMPO_WRITE = "[w] time:%.3f sid:%s rid:%d lvl:%2d bid:%d size[B]:%d"


import os
import sys
import time
import sqlite3
import json
import pandas as pd
from functools import wraps
from .apisession import APISession, EPOCHS_MAX


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


class SQLiteSession(APISession):
    def __init__(self, path=None, workers=16, cert=None):
        """
        Parameters
        ----------
        path : str, optional
            location for the sqlite database
        workers : int
            default 16
        """
        super(SQLiteSession, self).__init__(workers=workers, cert=cert)

        if path is None:
            path = os.path.expanduser("~") # $HOME
        if sys.platform == "win32":
            self.home = os.path.join(path, "tmpo")
        else:
            self.home = os.path.join(path, ".tmpo")
        self.db = os.path.join(self.home, "tmpo.sqlite3")

        try:
            os.mkdir(self.home)
        except OSError:  # dir exists
            pass

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
        self.dbcur.execute(SQL_TMPO_DEL, (sid,))

    @dbcon
    def reset(self, sid):
        """
        Removes all tmpo blocks for a given sensor, but keeps sensor table
        intact, so sensor id and token remain in the database.

        Parameters
        ----------
        sid : str
        """
        self.dbcur.execute(SQL_TMPO_DEL, (sid,))

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
                rid, lvl, bid, ext = last
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
               datetime=True, **kwargs):
        """
        Create data Series

        Parameters
        ----------
        sid : str
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
    def first_timestamp(self, sid, epoch=False, **kwargs):
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
        if not epoch:
            timestamp = self._epoch2timestamp(timestamp)
        return timestamp

    @dbcon
    def _last_block(self, sid, **kwargs):
        cur = self.dbcur.execute(SQL_TMPO_LAST_DATA, (sid,))
        row = cur.fetchone()
        if row is None:
            return None

        rid, lvl, bid, ext, blk = row

        jblk = self._decompress_block(blk, ext)
        data = json.loads(jblk.decode('UTF-8'))
        return data

    def _req_sync(self, sid, rid, lvl, bid):
        self.dbcur.execute(SQL_SENSOR_TOKEN, (sid,))
        token, = self.dbcur.fetchone()
        blist = self._req_blocklist(sid=sid, token=token, rid=rid, lvl=lvl,
                                    bid=bid)
        fs = self._req_blocks(sid=sid, token=token, blist=blist)
        for (t, f) in fs:
            self._write_block(
                f.result(), sid, t["rid"], t["lvl"], t["bid"], t["ext"])

    def _write_block(self, r, sid, rid, lvl, bid, ext):
        blk = sqlite3.Binary(r.content)
        now = time.time()
        try:
            self.dbcur.execute(SQL_TMPO_INS, (sid, rid, lvl, bid, ext, now, blk))
        except sqlite3.IntegrityError:
            pass
        self._clean(sid, rid, lvl, bid)
        self._dprintf(DBG_TMPO_WRITE, now, sid, rid, lvl, bid, len(blk))

    def _clean(self, sid, rid, lvl, bid):
        if lvl == 8:
            return
        lastchild = self._lastchild(lvl, bid)
        self.dbcur.execute(SQL_TMPO_CLEAN, (sid, rid, lvl - 4, lastchild))
        self._clean(sid, rid, lvl - 4, lastchild)
