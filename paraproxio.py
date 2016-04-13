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

from asyncio import AbstractEventLoop, Future
from asyncio.futures import CancelledError
from typing import Tuple, Callable, Optional, List
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

DEFAULT_WORKING_DIR = '.paraproxio'
DEFAULT_BUFFER_DIR = os.path.join(DEFAULT_WORKING_DIR, 'buffer')
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


def need_file_to_parallel(path: str) -> bool:
    pr = urlparse(path)  # type: ParseResult
    path, ext = os.path.splitext(pr.path)
    return ext.lower() in files_to_parallel


def get_bytes_ranges(length: int, parts: int) -> List[Tuple[int, int]]:
    """ Get bytes ranges """
    ###################################################################################################
    #
    # length            = 89
    # parts             = 4
    # range_size        = length // parts = 89 // 4 = 22
    # last_range_size   = range_size + length % parts = 22 + 89 % 4 = 22 + 1 = 23
    #
    # [<-----range_size----->|<-----range_size----->|<-----range_size----->|<---last_range_size---->|
    # [**********************|**********************|**********************|**********************|*]
    # 0                      22                     44                     66                    88 89
    #
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


class RangeDownloader:
    def __init__(
            self,
            path: str,
            bytes_range: Tuple[int, int],
            buffer_file_path,
            *,
            loop: AbstractEventLoop = None,
            server_logger=server_logger,
            chunk_size=DEFAULT_CHUNK_SIZE,
            chunk_download_timeout=DEFAULT_CHUNK_DOWNLOAD_TIMEOUT):
        self._path = path
        self._bytes_range = bytes_range
        self._length = bytes_range[1] - bytes_range[0] + 1
        self.buffer_file_path = buffer_file_path
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

    async def download(self) -> str:
        # Prepare an empty buffer file.
        await self._loop.run_in_executor(None, self._create_buffer_file)

        try:
            # Create client session for downloading a file part from a host.
            async with aiohttp.ClientSession(loop=self._loop, headers=self._headers) as session:
                # Request a host for a file part.
                async with session.request('GET', self._path) as res:  # type: aiohttp.ClientResponse
                    if res.status != 206:
                        raise WrongResponseError('Expected status code 206, but {!s} ({!s}) received.',
                                                 res.status,
                                                 res.reason)

                    hrh = res.headers  # type: CIMultiDictProxy
                    # TODO: check headers.

                    # Read content by chunks and write to the buffer file.
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

    def _debug(self, msg, *args, **kwargs):
        msg = "{!r} {!s}".format(self, msg)
        self._server_logger.debug(msg, *args, **kwargs)

    def cancel(self):
        if self._state != DOWNLOADING:
            return
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
        f = open(self.buffer_file_path, 'xb')
        f.seek(self._length - 1)
        f.write(b'0')
        f.flush()
        f.seek(0)
        self._buffer_file = f

    def __repr__(self, *args, **kwargs):
        return '<RangeDownloader: [{0[0]!s}-{0[1]!s}] {1!r}>'.format(self._bytes_range, self._path)


class ParallelDownloader:
    def __init__(
            self,
            path: str,
            file_length: int,
            *,
            parallels: int = DEFAULT_PARALLELS,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            loop: AbstractEventLoop = None,
            buffer_dir: str = DEFAULT_BUFFER_DIR):
        assert parallels > 1

        self._path = path
        self._file_length = file_length
        self._parallels = parallels
        self._chunk_size = chunk_size
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._filename = None  # type: str
        self._download_dir = os.path.join(buffer_dir, str(self._loop.time()).replace('.', '_'))
        self._downloaders = []  # type: List[RangeDownloader]
        self._downloads = []  # type: List[Future]
        self._state = NOT_STARTED
        self._create_download_dir()

    @property
    def state(self) -> str:
        return self._state

    @property
    def downloaders(self):
        return self._downloaders

    async def download(self):
        assert self._state == NOT_STARTED
        self._state = DOWNLOADING

        # Calculate bytes ranges.
        bytes_ranges = get_bytes_ranges(self._file_length, self._parallels)

        # Create a downloader for each bytes range.
        for i, bytes_range in enumerate(bytes_ranges):
            filename = '{idx:02}_{range[0]!s}-{range[1]!s}.tmp'.format(idx=i, range=bytes_range)
            buffer_file_path = os.path.join(self._download_dir, filename)
            self._downloaders.append(
                RangeDownloader(self._path,
                                bytes_range,
                                buffer_file_path,
                                loop=self._loop,
                                chunk_size=self._chunk_size))

        # Start downloaders.
        for downloader in self._downloaders:
            self._downloads.append(asyncio.ensure_future(downloader.download(), loop=self._loop))

        # Waiting for all downloads to complete.
        try:
            pending = set(self._downloads)
            while self._state is DOWNLOADING and pending:
                done, pending = await asyncio.wait(pending, loop=self._loop, return_when=asyncio.FIRST_COMPLETED)
                for dd in done:  # type: Future
                    if dd.result() is not DOWNLOADED:
                        raise CancelledError()

        except Exception as ex:
            self.cancel()
            raise
        else:
            # OK. All done.
            self._state = DOWNLOADED

    async def read(self, callback: Callable[[bytearray], None]):
        chunk = bytearray(self._chunk_size)
        chunk_size = len(chunk)
        for downloader in self._downloaders:
            with open(downloader.buffer_file_path, 'rb') as file:
                while True:
                    r = await self._run_nonblocking(lambda: file.readinto(chunk))
                    if not r:
                        break
                    if r < chunk_size:
                        callback(memoryview(chunk)[:r].tobytes())
                    else:
                        callback(chunk)

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
        await self._run_nonblocking(lambda: shutil.rmtree(self._download_dir, ignore_errors=True))

    async def _run_nonblocking(self, func):
        return await self._loop.run_in_executor(None, lambda: func())

    def _create_download_dir(self):
        if not os.path.exists(self._download_dir):
            os.makedirs(self._download_dir)

    def __repr__(self, *args, **kwargs):
        return '<ParallelDownloader: {!r}>'.format(self._path)


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
            chunk_size: int = DEFAULT_CHUNK_SIZE,
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
        self._chunk_size = chunk_size

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
        if content_length <= 0:
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

        pd = ParallelDownloader(message.path, content_length,
                                parallels=self._parallels, chunk_size=self._chunk_size, loop=self._loop)
        try:
            await pd.download()
            await pd.read(lambda chunk: client_res.write(chunk))
            await client_res.write_eof()
        except Exception as exc:
            self.log_debug("CANCELLED PARALLEL GET {!r}.".format(message.path))
            raise
        finally:
            await pd.clear()

        return client_res

    async def get_file_head(self, path: str) -> Optional[CIMultiDictProxy]:
        """Make a HEAD request to get a 'content-length' and 'accept-ranges' headers."""
        try:
            async with aiohttp.ClientSession(loop=self._loop) as session:
                async with session.request(hdrs.METH_HEAD, path) as res:  # type: aiohttp.ClientResponse
                    return res.headers
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError):
            self.log_debug("Could not get a HEAD for the {!r}.".format(path))
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
            await asyncio.sleep(sleep, loop=self._loop)
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
                await asyncio.wait_for(
                    self._connections_cleanup(), timeout, loop=self._loop)
            except asyncio.TimeoutError:
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


def setup_dirs(*dirs):
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def setup_logging(
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
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(level)
    server_logger.addHandler(ch)
    access_logger.addHandler(ch)

    # Server log file handler.
    slfh = logging.FileHandler(os.path.join(logs_dir, server_log_filename))
    slfh.setLevel(level)
    server_logger.addHandler(slfh)

    # Access log file handler.
    alfh = logging.FileHandler(os.path.join(logs_dir, access_log_filename))
    alfh.setLevel(level)
    access_logger.addHandler(alfh)


def get_args(args):
    parser = argparse.ArgumentParser(prog="paraproxio",
                                     description="An HTTP proxy with a parallel downloading of big files.")
    parser.add_argument("-H", "--host", type=str, default=DEFAULT_HOST, help="host address")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_PORT, help="port")
    parser.add_argument("--parallels", type=int, default=DEFAULT_PARALLELS, help="parallel downloads of a big file")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="max workers of executor")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="chunk size")
    parser.add_argument("--buffer-dir", type=str, default=DEFAULT_BUFFER_DIR, help="buffer dir")
    parser.add_argument("--logs-dir", type=str, default=DEFAULT_LOGS_DIR, help="logs dir")
    parser.add_argument("--debug", default=False, action="store_true", help="enable debug information in the stdout")
    parser.add_argument("--version", action="version", version=PARAPROXIO_VERSION)
    return parser.parse_args(args)


def run(args=None, loop=None):
    args = get_args(args)

    setup_dirs(args.buffer_dir, args.logs_dir)
    setup_logging(args.logs_dir,
                  DEFAULT_SERVER_LOG_FILENAME,
                  DEFAULT_ACCESS_LOG_FILENAME,
                  debug=args.debug)

    autoclose_loop = False

    # Create an event loop.
    if loop is None:
        autoclose_loop = True
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        # Create custom executor.
        executor = concurrent.futures.ThreadPoolExecutor(args.max_workers)
        loop.set_default_executor(executor)

    handler_factory = ParallelHttpRequestHandlerFactory(loop=loop, debug=args.debug,
                                                        parallels=args.parallels,
                                                        chunk_size=args.chunk_size,
                                                        keep_alive=75)

    srv = loop.run_until_complete(loop.create_server(handler_factory, args.host, args.port))
    print('Paraproxio serving on', srv.sockets[0].getsockname())
    if args.debug:
        print('Debug mode.')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        loop.run_until_complete(handler_factory.finish_connections(timeout=15))

    if autoclose_loop:
        loop.close()


if __name__ == '__main__':
    run()
