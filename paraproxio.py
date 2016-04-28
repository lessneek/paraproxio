#!/usr/bin/python
"""
Paraproxio is an HTTP proxy with a parallel downloading of big files.
"""
# Copyright (C) 2016 Alexander Logger <intagger@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__version__ = '1.0'
PARAPROXIO_VERSION = "Paraproxio/" + __version__

import sys

req_ver = (3, 5)
if sys.version_info < req_ver:
    print('Minimum Python/{0[0]}.{0[1]} required. You run this script with Python/{1[0]}.{1[1]}.'.format(
        req_ver, sys.version_info),
        file=sys.stderr)
    exit(1)

import asyncio
import logging
import os
import shutil
import argparse
import concurrent.futures
from collections import namedtuple
import json
from time import time

from asyncio import AbstractEventLoop, Future, ensure_future, wait, wait_for, TimeoutError
from asyncio.futures import CancelledError
from typing import Tuple, Callable, Optional, List, Set, Dict, Any
from urllib.parse import urlparse, ParseResult

try:
    import aiohttp
    import aiohttp.hdrs as hdrs
    import aiohttp.server
    from aiohttp.multidict import CIMultiDictProxy
    from aiohttp.protocol import RawRequestMessage
    from aiohttp.streams import EmptyStreamReader
except ImportError as err:
    print(
        "Required module '{0}' not found. Try to run 'pip install {0}' to install it.".format(err.name),
        file=sys.stderr)
    exit(1)

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8880

DEFAULT_CHUNK_DOWNLOAD_TIMEOUT = 10
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_PARALLELS = 32
DEFAULT_MAX_WORKERS = 8
DEFAULT_PART_SIZE = DEFAULT_CHUNK_SIZE * DEFAULT_PARALLELS * DEFAULT_MAX_WORKERS

DEFAULT_WORKING_DIR = '.paraproxio'
DEFAULT_BUFFER_DIR = os.path.join(DEFAULT_WORKING_DIR, 'buffer')
DEFAULT_CACHE_DIR = os.path.join(DEFAULT_WORKING_DIR, 'cache')
DEFAULT_LOGS_DIR = os.path.join(DEFAULT_WORKING_DIR, 'logs')
DEFAULT_SERVER_LOG_FILENAME = 'paraproxio.server.log'
DEFAULT_ACCESS_LOG_FILENAME = 'paraproxio.access.log'

DEFAULT_PARACCESS_LOG_FORMAT = '%a %l %u %t "%r" %s %b "%{Referrer}i" "%{User-Agent}i" %{X-Parallels}o'

PARALLELS_HEADER = 'X-Parallels'  # Used in responses. Value: number of parallel downloads used.

server_logger = logging.getLogger('paraproxio.server')
access_logger = logging.getLogger('paraproxio.access')

NOT_STARTED = 'NOT STARTED'
DOWNLOADING = 'DOWNLOADING'
DOWNLOADED = 'DOWNLOADED'
CANCELLED = 'CANCELLED'

_DOWNLOADER_STATES = {NOT_STARTED, DOWNLOADING, DOWNLOADED, CANCELLED}

files_to_parallel = ['.iso', '.zip', '.rpm', '.gz']

CACHE_INFO_FILE_NAME = 'info.json'
CACHE_BIN_FILE_NAME = 'file.bin'


def need_file_to_parallel(url: str) -> bool:
    pr = urlparse(url)  # type: ParseResult
    url, ext = os.path.splitext(pr.path)
    return ext.lower() in files_to_parallel


def get_bytes_ranges_by_parts(length: int, parts: int) -> List[Tuple[int, int]]:
    """ Get bytes ranges """
    ###################################################################################################
    #
    # length            = 89
    # parts             = 5
    # range_size        = length // parts = 89 // 5 = 17
    # last_range_size   = range_size + length % parts = 17 + 89 % 5 = 17 + 4 = 21
    #
    # [<--range_size--->|<--range_size--->|<--range_size--->|<--range_size--->|<--last_range_size--->|
    # [*****************|*****************|*****************|*****************|*****************|****]
    # 0                 17                34                51                68                85   89
    #
    ###################################################################################################
    range_size = length // parts
    last_range_size = range_size + length % parts
    last_range_idx = parts - 1
    bytes_ranges = []
    for part in range(0, last_range_idx):
        bytes_range = (part * range_size, ((part + 1) * range_size) - 1)
        bytes_ranges.append(bytes_range)
    last_range_offset = last_range_idx * range_size
    bytes_ranges.append((last_range_offset, last_range_offset + last_range_size - 1))
    return bytes_ranges


def get_bytes_ranges_by_part_size(length: int, part_size: int) -> List[Tuple[int, int]]:
    bytes_ranges = []
    for offset in range(0, length, part_size):
        left = length - offset
        if left < part_size:
            part_size = left
        bytes_ranges.append((offset, offset + part_size - 1))
    return bytes_ranges


def get_unique_name():
    return str(time()).replace('.', '_')


class RangeDownloader:
    def __init__(
            self,
            url: str,
            bytes_range: Tuple[int, int],
            buffer_file_path,
            *,
            loop: AbstractEventLoop = None,
            server_logger=server_logger,
            chunk_size=DEFAULT_CHUNK_SIZE,
            chunk_download_timeout=DEFAULT_CHUNK_DOWNLOAD_TIMEOUT):
        self._url = url
        self._bytes_range = bytes_range
        self._length = bytes_range[1] - bytes_range[0] + 1
        self._buffer_file_path = buffer_file_path
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._server_logger = server_logger
        self._chunk_size = chunk_size
        self._chunk_download_timeout = chunk_download_timeout
        self._headers = {'Range': 'bytes={0[0]!s}-{0[1]!s}'.format(self._bytes_range)}
        self._bytes_downloaded = 0
        self._state = NOT_STARTED

    @property
    def state(self) -> str:
        return self._state

    @property
    def bytes_downloaded(self):
        return self._bytes_downloaded

    @property
    def buffer_file_path(self):
        return self._buffer_file_path

    async def download(self) -> str:
        if self._state != NOT_STARTED:
            return self._state

        # Prepare an empty buffer file.
        await self._loop.run_in_executor(None, self._create_buffer_file)

        try:
            # Create client session for downloading a file part from a host.
            async with aiohttp.ClientSession(loop=self._loop, headers=self._headers) as session:
                # Request a host for a file part.
                async with session.request('GET', self._url) as res:  # type: aiohttp.ClientResponse
                    if res.status != 206:
                        raise WrongResponseError('Expected status code 206, but {!s} ({!s}) received.',
                                                 res.status,
                                                 res.reason)

                    hrh = res.headers  # type: CIMultiDictProxy
                    # TODO: check headers.

                    # Read content by chunks and write to the buffer file.
                    if self._state == NOT_STARTED:
                        self._state = DOWNLOADING
                    while self._state is DOWNLOADING:
                        with aiohttp.Timeout(self._chunk_download_timeout, loop=self._loop):
                            chunk = await res.content.read(self._chunk_size)
                            self._bytes_downloaded += len(chunk)

                            self._debug("Read ({!s} bytes). Downloaded: {!s} of {!s} bytes. [{:.2%}]".format(
                                len(chunk), self._bytes_downloaded, self._length,
                                self._bytes_downloaded / self._length))

                            if not chunk:
                                self._state = DOWNLOADED
                                break
                            await self._write_chunk(chunk)
                    await self._flush_and_release()
                    if self._state != DOWNLOADED:
                        res.close()  # Close the response if not downloaded.
        except aiohttp.ServerDisconnectedError as exc:
            self._debug('Server disconnected error: {!r}.'.format(exc))
            self.cancel()
        except WrongResponseError as exc:
            self._debug('Wrong response error: {!r}.'.format(exc))
            self.cancel()
        except asyncio.TimeoutError:
            self._debug('Timeout.')
            self.cancel()
        except Exception as exc:
            self._debug('Unexpected exception: {!r}.'.format(exc))
            self.cancel()
        finally:
            return self._state

    def cancel(self):
        self._debug('Cancel called.')
        self._state = CANCELLED

    async def _write_chunk(self, chunk):
        await self._run_nonblocking(lambda: self._buffer_file.write(chunk))

    def _flush_and_release(self):
        def flush_and_release():
            if not self._buffer_file:
                return
            self._buffer_file.flush()
            self._buffer_file.close()
            del self._buffer_file

        return self._run_nonblocking(flush_and_release)

    def _run_nonblocking(self, func):
        return self._loop.run_in_executor(None, lambda: func())

    def _create_buffer_file(self):
        f = open(self._buffer_file_path, 'xb')
        f.seek(self._length - 1)
        f.write(b'0')
        f.flush()
        f.seek(0)
        self._buffer_file = f

    def _debug(self, msg, *args, **kwargs):
        msg = "{!r} {!s}".format(self, msg)
        self._server_logger.debug(msg, *args, **kwargs)

    def __repr__(self, *args, **kwargs):
        return '<RangeDownloader ({2!s}): [{0[0]!s}-{0[1]!s}] {1!r}>'.format(self._bytes_range, self._url, self._state)


class ParallelDownloader:
    """Parallel downloader"""

    _state = NOT_STARTED
    _downloaders = []  # type: List[RangeDownloader]
    _downloads = set()  # type: Set[Future]
    _filename = None  # type: str
    _next_id = 0

    def __init__(
            self,
            url: str,
            file_length: int,
            *,
            parallels: int = DEFAULT_PARALLELS,
            part_size: int = DEFAULT_PART_SIZE,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            loop: AbstractEventLoop = None,
            server_logger=server_logger,
            buffer_dir: str = DEFAULT_BUFFER_DIR):
        assert parallels > 1

        self._url = url
        self._file_length = file_length
        self._part_size = part_size
        self._parallels = parallels
        self._chunk_size = chunk_size
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._server_logger = server_logger
        self._download_dir = os.path.join(buffer_dir, get_unique_name())

        self._create_download_dir()

        # Calculate bytes ranges.
        self._bytes_ranges = get_bytes_ranges_by_part_size(self._file_length, self._part_size)
        self._parts = len(self._bytes_ranges)

        if self._parts < self._parallels:
            self._bytes_ranges = get_bytes_ranges_by_parts(self._file_length, self._parallels)
            self._parts = len(self._bytes_ranges)

        self._state_condition = asyncio.Condition(loop=self._loop)

    @property
    def state(self) -> str:
        return self._state

    @property
    def downloaders(self):
        return self._downloaders

    async def download(self):
        if self._state == DOWNLOADING:
            return
        self._state = DOWNLOADING

        # Create a downloader for each bytes range.
        for i, bytes_range in enumerate(self._bytes_ranges):
            filename = '{idx:03}_{range[0]!s}-{range[1]!s}.tmp'.format(idx=i, range=bytes_range)
            buffer_file_path = os.path.join(self._download_dir, filename)
            self._downloaders.append(
                RangeDownloader(self._url,
                                bytes_range,
                                buffer_file_path,
                                loop=self._loop,
                                chunk_size=self._chunk_size))

        # Start first single downloader for fast first part response to a client.
        self._start_next_downloaders(1)

        # Waiting for all downloads to complete.
        try:
            while self._state is DOWNLOADING and self._downloads:
                done, self._downloads = await wait(self._downloads, loop=self._loop,
                                                   return_when=asyncio.FIRST_COMPLETED)
                for dd in done:  # type: Future
                    # Cancel downloading if any of completed downloads is not downloaded.
                    if dd.result() is not DOWNLOADED:
                        raise CancelledError()

                self._start_next_downloaders()

                # Notify all readers.
                async with self._state_condition:
                    self._state_condition.notify_all()

        except Exception as ex:
            self._debug('Download failed. Error: {!r}.'.format(ex))
            self.cancel()

            # Notify all readers.
            async with self._state_condition:
                self._state_condition.notify_all()

            raise DownloadError(ex)
        else:
            # OK. All done.
            self._state = DOWNLOADED

    def _start_next_downloaders(self, n=None):
        """Start next downloaders if needed according with parallels count."""
        if not n:
            n = self._parallels - len(self._downloads)
        while n > 0 and self._next_id < self._parts:
            downloader = self._downloaders[self._next_id]
            self._next_id += 1
            self._downloads.add(ensure_future(downloader.download(), loop=self._loop))
            n -= 1

    async def read(self, callback: Callable[[bytearray], Any]):
        try:
            for downloader in self._downloaders:

                # Wait until downloader is not in a downloaded/cancelled state.
                async with self._state_condition:
                    while downloader.state not in (DOWNLOADED, CANCELLED):
                        await self._state_condition.wait()
                    if downloader.state != DOWNLOADED:
                        self._debug('Downloader not in `DOWNLOADED` state, but in `{!s}`.'.format(downloader.state))
                        raise CancelledError()

                # Open file and send all its bytes it to back.
                await read_from_file_by_chunks(downloader.buffer_file_path, callback, self._chunk_size,
                                               lambda: self._state != CANCELLED, loop=self._loop)
        except Exception as exc:
            raise ReadError(exc)

    def cancel(self):
        if self._state != DOWNLOADING:
            return
        self._state = CANCELLED
        for downloader in self._downloaders:  # type: RangeDownloader
            downloader.cancel()

    async def clear(self):
        if self._state not in (DOWNLOADED, CANCELLED):
            return
        await self._clear()

    async def _clear(self):
        self._downloaders.clear()
        await self._run_nonblocking(lambda: shutil.rmtree(self._download_dir, ignore_errors=True))

    async def _run_nonblocking(self, func):
        return await self._loop.run_in_executor(None, lambda: func())

    def _create_download_dir(self):
        if not os.path.exists(self._download_dir):
            os.makedirs(self._download_dir, exist_ok=True)

    def _debug(self, msg, *args, **kwargs):
        msg = "{!r} {!s}".format(self, msg)
        self._server_logger.debug(msg, *args, **kwargs)

    def __repr__(self, *args, **kwargs):
        return '<ParallelDownloader: {!r}>'.format(self._url)


CacheFileInfo = namedtuple('CacheFileInfo', ['url', 'length', 'last_modified', 'etag'])

INITIALIZING = 'INITIALIZING'
READY = 'READY'


def get_cache_bin_file_path(cache_entry_dir: str):
    return os.path.join(cache_entry_dir, CACHE_BIN_FILE_NAME)


def get_cache_info_file_path(cache_entry_dir: str):
    return os.path.join(cache_entry_dir, CACHE_INFO_FILE_NAME)


def create_new_cache_entry_dir(cache_dir: str):
    cache_entry_dir = os.path.join(cache_dir, get_unique_name())
    os.makedirs(cache_entry_dir, exist_ok=True)
    return cache_entry_dir


async def read_from_file_by_chunks(
        file_path: str,
        callback: Callable[[bytearray], None],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        condition: Callable[[], bool] = lambda: True,
        *,
        loop):
    chunk = bytearray(chunk_size)
    with open(file_path, 'rb') as f:
        while condition():
            r = await loop.run_in_executor(None, lambda: f.readinto(chunk))
            if not r:
                break
            if r < chunk_size:
                callback(memoryview(chunk)[:r].tobytes())
            else:
                callback(chunk)


class CachingDownloader:
    """Downloader with caching."""

    _state = INITIALIZING

    _downloadings = {}  # type: Dict[str, Tuple[ParallelDownloader, Future, Future]]
    _uploadings = set()  # type: Set[(str, Future)]
    _cache = {}  # type: Dict[str, str]

    def __init__(self,
                 cache_dir: str,
                 parallels,
                 part_size,
                 chunk_size,
                 loop,
                 server_logger=server_logger):
        self._cache_dir = cache_dir
        self._parallels = parallels
        self._part_size = part_size
        self._chunk_size = chunk_size
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._server_logger = server_logger
        self._state_condition = asyncio.Condition(loop=self._loop)

        ensure_future(self._init_cache(), loop=self._loop)

    async def download(self, url: str, head: CIMultiDictProxy, callback: Callable[[bytearray], None]) -> str:
        await self._when_state(READY)

        if not await self._upload_from_cache(url, head, callback):
            await self._upload_with_pd(url, head, callback)

    async def _upload_from_cache(self, url: str, head: CIMultiDictProxy, callback: Callable[[bytearray], None]) -> bool:
        """Upload from cache"""
        cache_entry_dir = self._cache.get(url)
        if not cache_entry_dir:
            return False

        cache_info = await self._load_cache_info(cache_entry_dir)

        up_to_date = head.get(hdrs.LAST_MODIFIED) == cache_info.get(hdrs.LAST_MODIFIED)
        up_to_date &= head.get(hdrs.ETAG) == cache_info.get(hdrs.ETAG)

        if not up_to_date:
            self._debug('Outdated cache for: {!r} deleted.'.format(url))
            await self._delete_cache_entry(cache_entry_dir)
            return False

        self._debug('Uploading (from cache): {!r}.'.format(url))

        cache_bin_file_path = get_cache_bin_file_path(cache_entry_dir)

        # Open file and send all its bytes it to back.
        coro_read = read_from_file_by_chunks(cache_bin_file_path, callback, self._chunk_size,
                                             lambda: self._state != CANCELLED, loop=self._loop)
        uploading = ensure_future(coro_read, loop=self._loop)

        self._uploadings.add((url, uploading))

        try:
            await uploading
        except ReadError:
            self._debug('Read error.')
            pass
        except Exception as exc:
            self._debug('Uploading failed with exception: {!r}.'.format(exc))
            raise
        finally:
            self._uploadings.remove((url, uploading))
        return True

    async def _upload_with_pd(self, url, head: CIMultiDictProxy, callback: Callable[[bytearray], None]):
        """Upload using parallel downloader."""

        content_length = head.get(hdrs.CONTENT_LENGTH)
        assert content_length is not None
        content_length = int(content_length)

        self._debug('Uploading (from parallel downloader): {!r}.'.format(url))

        # Start or join existed downloading task.
        dl = self._downloadings.get(url)
        if dl:
            pd, downloading, caching = dl
        else:
            def downloading_done(_):
                self._debug('Downloading {!r} done.'.format(url))
                assert url in self._downloadings
                del self._downloadings[url]

            # Create parallel downloader.
            pd = ParallelDownloader(url, content_length,
                                    parallels=self._parallels,
                                    part_size=self._part_size,
                                    chunk_size=self._chunk_size,
                                    loop=self._loop)

            # Start downloading.
            downloading = ensure_future(pd.download(), loop=self._loop)
            downloading.add_done_callback(downloading_done)

            # Write downloading content to a cache entry.
            cache_entry_dir = create_new_cache_entry_dir(self._cache_dir)

            async def cache():
                try:
                    cache_bin_file_path = get_cache_bin_file_path(cache_entry_dir)
                    with open(cache_bin_file_path, 'xb') as cache_bin_file:
                        await pd.read(lambda chunk: cache_bin_file.write(chunk))

                    cache_info = {
                        'URL': url,
                        hdrs.CONTENT_LENGTH: content_length,
                        hdrs.LAST_MODIFIED: head.get(hdrs.LAST_MODIFIED),
                        hdrs.ETAG: head.get(hdrs.ETAG)
                    }
                    await self._save_cache_info(cache_entry_dir, cache_info)

                    # Add to a cache index.
                    self._cache[url] = cache_entry_dir
                except:
                    # Remove cache entry dir in case of error or cancellation.
                    await self._delete_cache_entry(cache_entry_dir)
                    raise

            caching = ensure_future(cache(), loop=self._loop)
            self._downloadings[url] = pd, downloading, caching

        uploading = ensure_future(pd.read(callback), loop=self._loop)
        self._uploadings.add((url, uploading))

        try:
            await uploading
        except ReadError:
            self._debug('Read error.')
            pass
        except Exception as exc:
            self._debug('Uploading failed with exception: {!r}.'.format(exc))
            raise
        finally:
            self._uploadings.remove((url, uploading))
            if not self._uploadings:
                await pd.clear()

    async def _save_cache_info(self, cache_entry_dir: str, cache_info: Dict[str, str]):
        def do():
            cache_info_file_path = get_cache_info_file_path(cache_entry_dir)
            with open(cache_info_file_path, 'w') as f:
                json.dump(cache_info, f, indent=4)

        await self._run_nb(do)

    async def _load_cache_info(self, cache_entry_dir: str) -> Dict[str, str]:
        def do():
            cache_info_file_path = get_cache_info_file_path(cache_entry_dir)
            if not os.path.isfile(cache_info_file_path):
                raise CacheError('Cache info file problem.')
            with open(cache_info_file_path) as f:
                return json.load(f)  # type: Dict

        return await self._run_nb(do)

    async def _init_cache(self):
        for cache_entry_dir in os.listdir(self._cache_dir):
            cache_entry_dir = os.path.join(self._cache_dir, cache_entry_dir)
            if not os.path.isdir(cache_entry_dir):
                continue
            try:
                cache_bin_file_path = get_cache_bin_file_path(cache_entry_dir)
                if not os.path.isfile(cache_bin_file_path):
                    raise CacheError('Cache bin file problem.')

                cache_info = await self._load_cache_info(cache_entry_dir)
                url = cache_info.get('URL')
                if not url:
                    raise CacheError('Bad cache info file.')

                self._cache[url] = cache_entry_dir
            except Exception as exc:
                self._debug('Cannot load cache from dir: {!r}. Error: {!r}.'.format(cache_entry_dir, exc))
                continue

        async with self._state_condition:
            self._state = READY
            self._state_condition.notify_all()

    async def _delete_cache_entry(self, cache_entry_dir):
        await self._run_nb(lambda: shutil.rmtree(cache_entry_dir))

    async def _when_state(self, state):
        async with self._state_condition:
            while self._state is not state:
                await self._state_condition.wait()

    async def _run_nb(self, func):
        return await self._loop.run_in_executor(None, lambda: func())

    def _debug(self, msg, *args, **kwargs):
        msg = "{!r} {!s}".format(self, msg)
        self._server_logger.debug(msg, *args, **kwargs)

    def __repr__(self, *args, **kwargs):
        return '<CachingDownloader (D:{!s} U:{!s})>'.format(len(self._downloadings), len(self._uploadings))


class ParallelHttpRequestHandler(aiohttp.server.ServerHttpProtocol):
    def __init__(
            self, manager, *, loop: AbstractEventLoop = None,
            keep_alive=75,
            keep_alive_on=True,
            timeout=0,
            server_logger=server_logger,
            access_logger=access_logger,
            access_log_format=DEFAULT_PARACCESS_LOG_FORMAT,
            debug=False,
            log=None,
            parallels: int = DEFAULT_PARALLELS,
            part_size: int = DEFAULT_PART_SIZE,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            buffer_dir: str = DEFAULT_BUFFER_DIR,
            cached_downloader: CachingDownloader,
            **kwargs):
        super().__init__(
            loop=loop,
            keep_alive=keep_alive,
            keep_alive_on=keep_alive_on,
            timeout=timeout,
            logger=server_logger,
            access_log=access_logger,
            access_log_format=access_log_format,
            debug=debug,
            log=log,
            **kwargs)

        self._manager = manager
        self._loop = loop
        self._parallels = parallels
        self._part_size = part_size
        self._chunk_size = chunk_size
        self._buffer_dir = buffer_dir
        self._cached_downloader = cached_downloader

    def connection_made(self, transport):
        super().connection_made(transport)
        self._manager.connection_made(self, transport)

    def connection_lost(self, exc):
        self._manager.connection_lost(self, exc)
        super().connection_lost(exc)

    def closing(self, timeout=15.0):
        super().closing(timeout)

    def check_request(self, message: RawRequestMessage):
        if message.method == hdrs.METH_CONNECT:
            self.handle_error(status=405, message=message)
            raise UnsupportedError("Method '%s' is not supported." % message.method)

    async def handle_request(self, message: RawRequestMessage, payload):
        now = self._loop.time()

        self.check_request(message)
        self.keep_alive(True)

        # Try to process parallel.
        response = await self.process_parallel(message, payload)

        # Otherwise process normally.
        if not response:
            response = await self.process_normally(message, payload)

        self.log_access(message, None, response, self._loop.time() - now)

    async def process_normally(self, message: RawRequestMessage, payload):
        """Process request normally."""
        req_data = payload if not isinstance(payload, EmptyStreamReader) else None

        # Request from a host.
        try:
            async with aiohttp.ClientSession(headers=message.headers, loop=self._loop) as session:
                async with session.request(message.method, message.path,
                                           data=req_data,
                                           allow_redirects=False) as host_resp:  # type: aiohttp.ClientResponse
                    client_res = aiohttp.Response(
                        self.writer, host_resp.status, http_version=message.version)

                    # Process host response headers.
                    for name, value in host_resp.headers.items():
                        if name == hdrs.CONTENT_ENCODING:
                            continue
                        if name == hdrs.CONTENT_LENGTH:
                            continue
                        if name == hdrs.TRANSFER_ENCODING:
                            if value.lower() == 'chunked':
                                client_res.enable_chunked_encoding()
                        client_res.add_header(name, value)

                    # Send headers to the client.
                    client_res.send_headers()

                    # Send a payload.
                    while True:
                        chunk = await host_resp.content.read(self._chunk_size)
                        if not chunk:
                            break
                        client_res.write(chunk)

                    if client_res.chunked or client_res.autochunked():
                        await client_res.write_eof()
            return client_res
        except aiohttp.ClientResponseError:
            self.log_debug("CANCELLED {!s} {!r}.".format(message.method, message.path))
            raise

    async def process_parallel(self, message: RawRequestMessage, payload) -> aiohttp.Response:
        """Try process a request parallel. Returns True in case of processed parallel, otherwise False."""
        # Checking the opportunity of parallel downloading.
        if message.method != hdrs.METH_GET or not need_file_to_parallel(message.path):
            return None
        head = await self.get_file_head(message.path)
        if head is None:
            return None
        accept_ranges = head.get(hdrs.ACCEPT_RANGES)
        if not accept_ranges or accept_ranges.lower() != 'bytes':
            return None
        content_length = head.get(hdrs.CONTENT_LENGTH)
        if content_length is None:
            return None
        content_length = int(content_length)
        if content_length <= 0 or content_length < DEFAULT_PART_SIZE:
            return None

        # All checks pass, start a parallel downloading.
        self.log_debug("PARALLEL GET {!r} [{!s} bytes].".format(message.path, content_length))

        # Get additional file info.
        content_type = head.get(hdrs.CONTENT_TYPE)

        # Prepare a response to a client.
        client_res = aiohttp.Response(self.writer, 200, http_version=message.version)
        client_res.add_header(hdrs.CONTENT_LENGTH, str(content_length))
        if content_type:
            client_res.add_header(hdrs.CONTENT_TYPE, content_type)
        client_res.add_header(PARALLELS_HEADER, str(self._parallels))
        client_res.send_headers()

        try:
            await self._cached_downloader.download(message.path, head, lambda chunk: client_res.write(chunk))
            await client_res.write_eof()
        except Exception as exc:
            self.log_debug("CANCELLED PARALLEL GET {!r}. Caused by exception: {!r}.".format(message.path, exc))
            raise

        return client_res

    async def get_file_head(self, url: str) -> Optional[CIMultiDictProxy]:
        """Make a HEAD request to get a 'content-length' and 'accept-ranges' headers."""
        self.log_debug('Getting a HEAD for url: {!s}.'.format(url))
        try:
            async with aiohttp.ClientSession(loop=self._loop) as session:
                async with session.request(hdrs.METH_HEAD, url) as res:  # type: aiohttp.ClientResponse
                    return res.headers
        except Exception as exc:
            self.log_debug("Could not get a HEAD for the {!r}. Error: {!r}.".format(url, exc))
        return None

    def get_client_address(self):
        address, port = self.transport.get_extra_info('peername')
        return '%s:%s' % (address, port)


class ParallelHttpRequestHandlerFactory:
    def __init__(self, *,
                 handler_class=ParallelHttpRequestHandler,
                 loop=None,
                 server_logger=server_logger,
                 access_logger=access_logger,
                 **kwargs):
        self._handler_class = handler_class
        self._loop = loop
        self._server_logger = server_logger
        self._access_logger = access_logger
        self._connections = {}
        self._kwargs = kwargs
        self.num_connections = 0

    @property
    def connections(self):
        return list(self._connections.keys())

    def connection_made(self, handler, transport):
        self._connections[handler] = transport

    def connection_lost(self, handler, exc=None):
        if handler in self._connections:
            del self._connections[handler]

    async def _connections_cleanup(self):
        sleep = 0.05
        while self._connections:
            await sleep(sleep, loop=self._loop)
            if sleep < 5:
                sleep *= 2

    async def finish_connections(self, timeout=None):
        # try to close connections in 90% of graceful timeout
        timeout90 = None
        if timeout:
            timeout90 = timeout / 100 * 90

        for handler in self._connections.keys():
            handler.closing(timeout=timeout90)

        if timeout:
            try:
                await wait_for(
                    self._connections_cleanup(), timeout, loop=self._loop)
            except TimeoutError:
                self._server_logger.warning(
                    "Not all connections are closed (pending: %d)",
                    len(self._connections))

        for transport in self._connections.values():
            transport.close()

        self._connections.clear()

    def __call__(self):
        self.num_connections += 1
        try:
            return self._handler_class(
                manager=self,
                loop=self._loop,
                server_logger=server_logger,
                access_logger=access_logger,
                **self._kwargs)
        except:
            server_logger.exception(
                'Can not create request handler: {!r}'.format(self._handler_class))


class ParaproxioError(Exception):
    pass


class WrongResponseError(ParaproxioError):
    pass


class UnsupportedError(ParaproxioError):
    pass


class DownloadError(ParaproxioError):
    pass


class ReadError(ParaproxioError):
    pass


class CacheError(ParaproxioError):
    pass


def setup_dirs(*dirs):
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def clean_dirs(*dirs):
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


def get_args(args):
    parser = argparse.ArgumentParser(prog="paraproxio",
                                     description="An HTTP proxy with a parallel downloading of big files.")
    parser.add_argument("-H", "--host", type=str, default=DEFAULT_HOST, help="host address")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_PORT, help="port")
    parser.add_argument("--parallels", type=int, default=DEFAULT_PARALLELS, help="parallel downloads of a big file")
    parser.add_argument("--part-size", type=int, default=DEFAULT_PART_SIZE, help="part size of a parallel download")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="max workers of executor")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="chunk size")
    parser.add_argument("--buffer-dir", type=str, default=DEFAULT_BUFFER_DIR, help="buffer dir")
    parser.add_argument("--cache-dir", type=str, default=DEFAULT_CACHE_DIR, help="cache dir")
    parser.add_argument("--logs-dir", type=str, default=DEFAULT_LOGS_DIR, help="logs dir")
    parser.add_argument("--debug", default=False, action="store_true", help="enable debug information in the stdout")
    parser.add_argument("--clean-all", default=False, action="store_true", help="clean all temp files before start")
    parser.add_argument("--version", action="version", version=PARAPROXIO_VERSION)
    pargs = parser.parse_args(args)

    pargs.buffer_dir = os.path.abspath(pargs.buffer_dir)
    pargs.cache_dir = os.path.abspath(pargs.cache_dir)
    pargs.logs_dir = os.path.abspath(pargs.logs_dir)

    return pargs


class Paraproxio:
    def __init__(self, args=None, loop=None, enable_logging=True):
        self._args = get_args(args)
        self._loop = loop
        self._enable_logging = enable_logging

        clean_dirs(self._args.buffer_dir)

        if self._args.clean_all:
            clean_dirs(self._args.logs_dir, self._args.cache_dir)

        setup_dirs(self._args.buffer_dir, self._args.logs_dir, self._args.cache_dir)

        # Create an event loop.
        self._autoclose_loop = False
        if self._loop is None:
            self._autoclose_loop = True
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(None)
            # Create custom executor.
            executor = concurrent.futures.ThreadPoolExecutor(self._args.max_workers)
            self._loop.set_default_executor(executor)

        if self._enable_logging:
            self._setup_logging(self._args.logs_dir,
                                DEFAULT_SERVER_LOG_FILENAME,
                                DEFAULT_ACCESS_LOG_FILENAME,
                                debug=self._args.debug)

        self._cached_downloader = CachingDownloader(cache_dir=self._args.cache_dir,
                                                    parallels=self._args.parallels,
                                                    part_size=self._args.part_size,
                                                    chunk_size=self._args.chunk_size,
                                                    loop=self._loop)

    def run_forever(self):
        handler_factory = ParallelHttpRequestHandlerFactory(loop=self._loop, debug=self._args.debug,
                                                            parallels=self._args.parallels,
                                                            part_size=self._args.part_size,
                                                            chunk_size=self._args.chunk_size,
                                                            buffer_dir=self._args.buffer_dir,
                                                            cached_downloader=self._cached_downloader,
                                                            keep_alive=75)

        srv = self._loop.run_until_complete(self._loop.create_server(handler_factory, self._args.host, self._args.port))
        print('Paraproxio serving on', srv.sockets[0].getsockname())
        if self._args.debug:
            print('Debug mode.')
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            srv.close()
            self._loop.run_until_complete(srv.wait_closed())
            self._loop.run_until_complete(handler_factory.finish_connections(timeout=15))

        if self._autoclose_loop:
            self._loop.close()

        if self._enable_logging:
            self._release_logging()

    def _setup_logging(
            self,
            logs_dir: str,
            server_log_filename: str,
            access_log_filename: str,
            *,
            debug=False):
        # Set levels.
        level = logging.DEBUG if debug else logging.INFO
        server_logger.setLevel(level)
        access_logger.setLevel(level)

        # stderr handler.
        self._stderr_handler = logging.StreamHandler(sys.stderr)
        self._stderr_handler.setLevel(level)
        server_logger.addHandler(self._stderr_handler)
        access_logger.addHandler(self._stderr_handler)

        # Server log file handler.
        self._sl_handler = logging.FileHandler(os.path.join(logs_dir, server_log_filename))
        self._sl_handler.setLevel(level)
        server_logger.addHandler(self._sl_handler)

        # Access log file handler.
        self._al_handler = logging.FileHandler(os.path.join(logs_dir, access_log_filename))
        self._al_handler.setLevel(level)
        access_logger.addHandler(self._al_handler)

    def _release_logging(self):
        server_logger.removeHandler(self._stderr_handler)
        access_logger.removeHandler(self._stderr_handler)
        server_logger.removeHandler(self._sl_handler)
        access_logger.removeHandler(self._al_handler)


if __name__ == '__main__':
    paraproxio = Paraproxio()
    paraproxio.run_forever()
