import pickle
import socket


DEFAULT_PORT = 9737


def sendmsg(address, data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(address)
        sock.sendall(pickle.dumps(data))
        received = sock.recv(10240)
        result = pickle.loads(received)
        if isinstance(result, Exception):
            raise result
        return result
    finally:
        sock.close()
