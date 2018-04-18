import sys

from server import Server

if __name__ == "__main__":
    s = Server(host=sys.argv[1], port=int(sys.argv[2]))
    s.run()
