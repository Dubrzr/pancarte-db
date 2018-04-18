from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler
from concurrent.futures import ThreadPoolExecutor

from handler import handle_query


class TCPHandler(BaseRequestHandler):

    def handle(self):
        def readlines(sock, delim, recv_buffer=1024):
            buffer = b''
            data = True
            while data:
                data = sock.recv(recv_buffer)
                buffer += data

                while buffer.find(delim) != -1:
                    line, buffer = buffer.split(delim, 1)
                    yield line.decode('utf-8')
            return

        for line in readlines(self.request, delim=b'\n'):
            self.request.sendall(handle_query(line).encode('utf-8'))


class Server:
    def __init__(self, host, port, max_workers=42):
        self.host = host
        self.port = port
        self.max_workers = max_workers

    def run(self):
        class PoolTCPServer(ThreadingMixIn, TCPServer):
            pool = ThreadPoolExecutor(max_workers=self.max_workers)

            def process_request(self, request, client_address):
                self.pool.submit(self.process_request_thread, request, client_address)

        with PoolTCPServer((self.host, self.port), TCPHandler) as tcp_server:
            tcp_server.serve_forever()
