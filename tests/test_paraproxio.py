#!/usr/bin/python
#
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

import asyncio
import concurrent.futures
import threading
import time
import unittest
from asyncio import BaseEventLoop
from typing import Optional
from urllib.parse import urlparse, ParseResult

import aiohttp.client
import aiohttp.server
from aiohttp import RawRequestMessage, hdrs

import paraproxio

TEST_WEB_SERVER_HOST = '127.0.0.1'
TEST_WEB_SERVER_PORT = 25580
TEST_WEB_SERVER_ADDRESS = 'http://%s:%s' % (TEST_WEB_SERVER_HOST, TEST_WEB_SERVER_PORT)

PROXY_SERVER_HOST = '127.0.0.1'
PROXY_SERVER_PORT = 28880
PROXY_ADDRESS = 'http://%s:%s' % (PROXY_SERVER_HOST, PROXY_SERVER_PORT)

TEST_WEB_SERVER_FILES = {'/testfile1.txt': bytes(range(0, 256))}
CHUNK_SIZE = 64 * 1024


class TestRequestHandler(aiohttp.server.ServerHttpProtocol):
    async def handle_request(self, message: RawRequestMessage, payload):
        if message.method != 'GET':
            return
        pr = urlparse(message.path)  # type: ParseResult
        filename = pr.path

        file_bytes = TEST_WEB_SERVER_FILES.get(filename)
        if file_bytes is None:
            self.handle_error(404, 'Not found.')

        file_len = len(file_bytes)
        client_res = aiohttp.Response(
            self.writer, 200, http_version=message.version)
        client_res.add_header(hdrs.CONTENT_LENGTH, str(file_len))
        client_res.send_headers()
        client_res.write(file_bytes)
        client_res.write_eof()


class LoopThread:
    def __init__(self):
        self._worker_thread = None
        self._loop = None  # type: Optional[BaseEventLoop]

    def run(self, loop, *args, **kwargs):
        pass

    def _run(self, *args, **kwargs):
        # Create custom executor.
        executor = concurrent.futures.ThreadPoolExecutor()

        # Create an event loop.
        loop = self._loop = asyncio.new_event_loop()
        loop.set_default_executor(executor)

        self.run(loop, *args, **kwargs)

    def start(self, *args, **kwargs):
        self._worker_thread = threading.Thread(target=self._run, name=self.name, args=args, kwargs=kwargs)
        self._worker_thread.start()

    def stop(self):
        loop = self._loop
        # self._loop = None
        if loop is not None:
            # loop.stop()
            loop.call_soon_threadsafe(lambda: loop.stop())
            while loop.is_running():
                time.sleep(1)
            loop.close()

    @property
    def name(self):
        return None

    @property
    def loop(self):
        return self._loop


class TestWebServer(LoopThread):
    def __init__(self):
        super().__init__()
        self._srv = None

    def run(self, *args, **kwargs):
        self._srv = self.loop.run_until_complete(
            self.loop.create_server(lambda: TestRequestHandler(loop=self.loop), TEST_WEB_SERVER_HOST,
                                    TEST_WEB_SERVER_PORT))
        self.loop.run_forever()

    def stop(self):
        self._srv.close()
        super().stop()

    @property
    def name(self):
        return 'TestWebServer'


class TestParaproxioServer(LoopThread):
    def __init__(self):
        super().__init__()

    def run(self, loop, *args, **kwargs):
        paraproxio.run(args=kwargs['args'], loop=loop)

    @property
    def name(self):
        return 'TestParaproxioServer'


def create_host_url(filename):
    return TEST_WEB_SERVER_ADDRESS + filename


class TestParaproxio(unittest.TestCase):
    def test_normal_get(self):
        # Start a web server.
        web_server = TestWebServer()
        web_server.start()

        # Start a proxy server.
        proxy_server = TestParaproxioServer()
        proxy_server.start(args=['--host', PROXY_SERVER_HOST, '--port', str(PROXY_SERVER_PORT)])

        # Make a test request to the web server through the proxy.
        async def test():
            await asyncio.sleep(3)
            connector = aiohttp.ProxyConnector(proxy=PROXY_ADDRESS)
            session = aiohttp.client.ClientSession(connector=connector)
            try:
                url = create_host_url('/testfile1.txt')
                async with session.get(url) as resp:  # type: aiohttp.ClientResponse
                    self.assertEqual(resp.status, 200)
                    content = await resp.read()
                    self.assertEqual(content, TEST_WEB_SERVER_FILES.get('/testfile1.txt'))
            finally:
                session.close()

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(asyncio.ensure_future(test()))
        except KeyboardInterrupt:
            pass
        finally:
            proxy_server.stop()
            web_server.stop()


if __name__ == "__main__":
    unittest.main()
