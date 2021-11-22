#!/usr/bin/python
#
# Copyright (C) 2016 Alexander Logger
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

import asyncio
import concurrent.futures
import os
import re
import threading
import time
import unittest
from asyncio import BaseEventLoop
from collections import namedtuple
from typing import Callable
from urllib.parse import urlparse, ParseResult

import aiohttp.client
import aiohttp.server
from aiohttp import RawRequestMessage, hdrs, ClientResponse

import paraproxio

TEST_WEB_SERVER_HOST = '127.0.0.1'
TEST_WEB_SERVER_PORT = 25580
TEST_WEB_SERVER_ADDRESS = 'http://%s:%s' % (TEST_WEB_SERVER_HOST, TEST_WEB_SERVER_PORT)

PROXY_SERVER_HOST = '127.0.0.1'
PROXY_SERVER_PORT = 28880
PROXY_ADDRESS = 'http://%s:%s' % (PROXY_SERVER_HOST, PROXY_SERVER_PORT)

CHUNK_SIZE = 64 * 1024

SMALL_FILE_PATH = '/testfile1.txt'
BIG_FILE_PATH = '/bigfile1.zip'

TEST_WEB_SERVER_FILES = {
    SMALL_FILE_PATH: os.urandom(1009),
    BIG_FILE_PATH: os.urandom(100000999)}

RANGERE = re.compile(r'^bytes=(\d+)-(\d+)$', re.I | re.A)

BytesRange = namedtuple('BytesRange', ['start', 'stop'])


class TestRequestHandler(aiohttp.server.ServerHttpProtocol):
    async def handle_request(self, message: RawRequestMessage, payload):
        if message.method not in ['GET', 'HEAD']:
            return

        head = message.method == 'HEAD'
        pr = urlparse(message.path)  # type: ParseResult
        filename = pr.path

        file_bytes = TEST_WEB_SERVER_FILES.get(filename)
        if file_bytes is None:
            await self.handle_error(404, 'Not found.')
            return

        content_length = file_len = len(file_bytes)

        bytes_range = message.headers.get('Range')
        try:
            if bytes_range:
                m = RANGERE.match(bytes_range)
                if not m:
                    raise RuntimeError('Wrong range.')
                bytes_range = BytesRange(int(m.group(1)), int(m.group(2)))
                content_length = bytes_range.stop - bytes_range.start + 1
                if content_length <= 0 or bytes_range.stop >= file_len:
                    raise RuntimeError('Wrong range.')

        except RuntimeError as exc:
            await self.handle_error(400, exc=exc)
            return

        status = 206 if bytes_range else 200

        client_res = aiohttp.Response(self.writer, status, http_version=message.version)
        client_res.add_header(hdrs.CONTENT_LENGTH, str(content_length))

        content = None

        if head:
            client_res.add_header(hdrs.ACCEPT_RANGES, 'bytes')
        else:
            if bytes_range:
                content = file_bytes[bytes_range.start:bytes_range.stop + 1]
                client_res.add_header(hdrs.CONTENT_RANGE, 'bytes {0[0]}-{0[1]}/{1}'.format(bytes_range, file_len))
            else:
                content = file_bytes

        client_res.send_headers()
        if content:
            client_res.write(content)
        await client_res.write_eof()


STOPPED = 'STOPPED'
STARTING = 'STARTING'
STARTED = 'STARTED'
STOPPING = 'STOPPING'


class LoopThread:
    def __init__(self):
        self._state = STOPPED
        self._worker_thread = None
        self._state_changed = threading.Condition()

    @property
    def name(self):
        return None

    @property
    def state(self):
        return self._state

    @property
    def loop(self):
        return self._loop

    def run(self, loop, *args, **kwargs):
        pass

    def _set_started(self):
        with self._state_changed:
            self._state = STARTED
            self._state_changed.notify_all()

    def _run(self, *args, **kwargs):
        try:
            with self._state_changed:
                if self._state != STARTING:
                    return

            # Create custom executor.
            executor = concurrent.futures.ThreadPoolExecutor()

            # Create an event loop.
            loop = self._loop = asyncio.new_event_loop()  # type: BaseEventLoop
            loop.set_default_executor(executor)
            asyncio.set_event_loop(None)

            # Schedule 'set started' on loop.
            loop.call_later(1, self._set_started)
            self.run(loop, *args, **kwargs)
        finally:
            with self._state_changed:
                self._state = STOPPED
                self._state_changed.notify_all()

    def start(self, wait_until_started=True, *args, **kwargs):
        with self._state_changed:
            if self._state != STOPPED:
                return
            self._state = STARTING
            self._state_changed.notify_all()

        self._worker_thread = threading.Thread(target=self._run, name=self.name, args=args, kwargs=kwargs)
        self._worker_thread.start()

        if wait_until_started:
            self.wait_until_started()

    def wait_until_started(self, timeout=None):
        with self._state_changed:
            while self._state != STARTED:
                self._state_changed.wait(timeout)

    def stopping(self):
        pass

    def stop(self):
        with self._state_changed:
            # Wait while starting state.
            while self._state == STARTING:
                self._state_changed.wait()
            if self._state == STOPPED or self._state == STOPPING:
                return
            self._state = STOPPING
            self._state_changed.notify_all()

        self.stopping()

        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(lambda: loop.stop())
            while loop.is_running():
                time.sleep(1)
            loop.close()


class TestWebServer(LoopThread):
    def __init__(self):
        super().__init__()
        self._srv = None

    def run(self, *args, **kwargs):
        self._srv = self.loop.run_until_complete(
            self.loop.create_server(lambda: TestRequestHandler(loop=self.loop), TEST_WEB_SERVER_HOST,
                                    TEST_WEB_SERVER_PORT))
        self.loop.run_forever()

    def close_server(self):
        if self._srv is not None:
            self._srv.close()

    def stopping(self):
        self.loop.call_soon_threadsafe(self.close_server)

    @property
    def name(self):
        return 'TestWebServer'


class TestParaproxioServer(LoopThread):
    def __init__(self):
        super().__init__()

    def run(self, loop, *args, **kwargs):
        ppx = paraproxio.Paraproxio(args=kwargs['args'], loop=loop)
        ppx.run_forever()

    @property
    def name(self):
        return 'TestParaproxioServer'


def create_host_url(filename):
    return TEST_WEB_SERVER_ADDRESS + filename


class TestParaproxio(unittest.TestCase):
    parallels = '3'

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        # Start a web server.
        self.web_server = TestWebServer()
        self.web_server.start()

        # Start a proxy server.
        self.proxy_server = TestParaproxioServer()
        self.proxy_server.start(
            args=['--host', PROXY_SERVER_HOST,
                  '--port', str(PROXY_SERVER_PORT),
                  '--parallels', self.parallels,
                  '--debug',
                  '--clean-all'])

    def tearDown(self):
        self.proxy_server.stop()
        self.web_server.stop()

    async def _go(self, file_path, check_resp: Callable[[ClientResponse], None] = None):
        """ Make a test request to the web server through the proxy."""
        expected_content = TEST_WEB_SERVER_FILES.get(file_path)
        expected_content_length = len(expected_content)
        connector = aiohttp.ProxyConnector(proxy=PROXY_ADDRESS, loop=self.loop)
        async with aiohttp.client.ClientSession(connector=connector, loop=self.loop) as session:
            url = create_host_url(file_path)
            async with session.get(url) as resp:  # type: ClientResponse
                self.assertEqual(resp.status, 200)
                if resp.headers.get(hdrs.TRANSFER_ENCODING) != 'chunked':
                    self.assertEqual(resp.headers.get(hdrs.CONTENT_LENGTH), str(expected_content_length))
                content = await resp.read()
                self.assertEqual(content, expected_content)
                if check_resp:
                    check_resp(resp)
                await asyncio.sleep(1, loop=self.loop)  # Wait a little bit before closing the session.

    def test_normal_get(self):
        def check_resp(resp: aiohttp.ClientResponse):
            self.assertEqual(resp.headers.get(paraproxio.PARALLELS_HEADER), None)

        self.loop.run_until_complete(self._go(SMALL_FILE_PATH, check_resp))

    def test_parallel_get(self):
        def check_resp(resp: aiohttp.ClientResponse):
            self.assertEqual(resp.headers.get(paraproxio.PARALLELS_HEADER), self.parallels)

        self.loop.run_until_complete(self._go(BIG_FILE_PATH, check_resp))


class TestGetBytesRanges(unittest.TestCase):
    def test_get_bytes_ranges_by_part_size(self):
        cases = [(100, 50, [(0, 49), (50, 99)]),
                 (101, 50, [(0, 49), (50, 99), (100, 100)]),
                 (102, 50, [(0, 49), (50, 99), (100, 101)]),
                 (103, 50, [(0, 49), (50, 99), (100, 102)]),
                 (101, 49, [(0, 48), (49, 97), (98, 100)])]

        for length, part_size, expected_bytes_ranges in cases:
            actual_bytes_ranges = paraproxio.get_bytes_ranges_by_part_size(length, part_size)
            self.assertEqual(actual_bytes_ranges, expected_bytes_ranges,
                             'length={!s}, part_size={!s}, expected_bytes_ranges={!r}, actual_bytes_ranges={!r}.'
                             .format(length, part_size, expected_bytes_ranges, actual_bytes_ranges))


if __name__ == "__main__":
    unittest.main()
