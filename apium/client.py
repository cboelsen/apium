# -*- coding: utf-8 -*-

"""
.. module: client
    :synopsis: Basic functions for interacting with the server.
"""


__all__ = ('sendmsg', )


import pickle
import socket


DEFAULT_PORT = 9737


def sendmsg(address, data):
    """Send the data to the given address.

    The data will be pickled before sending it, so the given data needs
    to be pickleable.

    :param address: The location and port of the server.
    :type address: tuple
    :returns: The server's response
    :rtype: object
    """
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
