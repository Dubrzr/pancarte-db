import sys

from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler
from concurrent.futures import ThreadPoolExecutor


class MyTCPHandler(BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print("{} wrote:".format(self.client_address[0]))
        print(self.data)
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

class PoolMixIn(ThreadingMixIn):
    def process_request(self, request, client_address):
        self.pool.submit(self.process_request_thread, request, client_address)

def server(host, port):
    class PoolTCPServer(PoolMixIn, TCPServer):
        pool = ThreadPoolExecutor(max_workers=40)

    with PoolTCPServer((host, port), MyTCPHandler) as tcp_server:
        tcp_server.serve_forever()

if __name__=="__main__":
    server(sys.argv[1], int(sys.argv[2]))