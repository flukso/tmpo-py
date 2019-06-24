import aiohttp
from typing import Union, List, Dict
import pandas as pd
import os
import ssl
import asyncio
import logging

from .apisession import APISession, EPOCHS_MAX, HTTP_ACCEPT, API_TMPO_SYNC, API_TMPO_BLOCK

class AsyncSession(APISession):
    _package_dir = os.path.dirname(__file__)
    _crt = os.path.join(_package_dir, ".flukso.crt")
    ssl_context = ssl.create_default_context(cafile=_crt)

    def __init__(self):
        self.session: aiohttp.ClientSession = None
        super(AsyncSession, self).__init__()

    async def series(self, sid: str, recycle_id: int=None,
                     head: Union[int, pd.Timestamp]=None,
                     tail: Union[int, pd.Timestamp]=None,
                     datetime: bool=True, token: str=None):
        token = token if token else self.sensors[sid]

        if head is None:
            head = 0
        else:
            head = self._2epochs(head)

        if tail is None:
            tail = EPOCHS_MAX
        else:
            tail = self._2epochs(tail)

        blist = await self._req_blocklist(sid=sid, token=token, rid=recycle_id)
        blist = self._slice_blist(blist=blist, head=head, tail=tail)
        if len(blist) == 0:
            return pd.Series([], name=sid)

        ts = await self._blklist_2_series(sid=sid, token=token, blist=blist, head=head, tail=tail)

        if datetime is True:
            ts.index = pd.to_datetime(ts.index, unit='s', utc=True)
        return ts

    async def _req_blocklist(self, sid: str, token: str, rid: int=0,
                             lvl: int=0, bid: int=0) -> List[Dict]:
        """Request the list of available blocks from the API"""

        headers = {
            "Accept": HTTP_ACCEPT["json"],
            "X-Token": token,
            "X-Version": "1.0"
        }
        params = {
            "rid": rid if rid else 0,
            "lvl": lvl if lvl else 0,
            "bid": bid if bid else 0}

        url = API_TMPO_SYNC % (self.host, sid)

        async with self.session.get(url=url, headers=headers, params=params, ssl=self.ssl_context) as response:
            j = await response.json()
        blist = sorted(j, key=lambda x: x['bid'])
        return blist

    async def _blklist_2_series(self, sid: str, token:str, blist: List[Dict], head: int, tail: int) -> pd.Series:
        series_list = await asyncio.gather(
            *[self._req_block_2_series(sid=sid, token=token, head=head, tail=tail, **h) for h in blist])

        logging.debug('Concat series')
        ts = pd.concat(series_list)
        if ts.empty:
            return pd.Series([], name=sid)
        ts.name = sid
        return ts

    async def _req_block_2_series(self, sid: str, token: str,
                                  rid: int, lvl: int, bid: int,
                                  ext: str, head: int, tail: int) -> pd.Series():
        #logging.debug(f'Requesting block {bid}')
        block = await self._req_block(sid=sid, token=token, rid=rid, lvl=lvl, bid=bid, ext=ext)
        #logging.debug(f'Parsing block {bid}')
        ts = self._blk2series(ext=ext, blk=block, head=head, tail=tail)
        #logging.debug(f'Done parsing block {bid}')
        return ts

    async def _req_block(self, sid: str, token: str, rid: int,
                         lvl: int, bid: int, ext: str):
        headers = {
            "Accept": HTTP_ACCEPT["gz"],
            "X-Token": token,
            "X-Version": "1.0"
        }
        url = API_TMPO_BLOCK % (self.host, sid, rid, lvl, bid)

        async with self.session.get(url=url, headers=headers, ssl=self.ssl_context) as response:
            block = await response.read()

        return block
