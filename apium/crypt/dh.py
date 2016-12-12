# -*- coding: utf-8 -*-

"""
.. module: dh
    :synopsis: Simple implementation of Diffie-Hellman.
"""


__all__ = ('DiffieHellman', )


import hashlib
from random import randint
from binascii import hexlify


class DiffieHellman(object):
    """A simple implementation of Diffie-Hellman for key-sharing."""

    # This prime comes from: http://tools.ietf.org/html/rfc3526#section-5
    modulus_p = 0xFFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864D87602733EC86A64521F2B18177B200CBBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFCE0FD108E4B82D120A92108011A723C12A787E6D788719A10BDBA5B2699C327186AF4E23C1A946834B6150BDA2583E9CA2AD44CE8DBBBC2DB04DE8EF92E8EFC141FBECAA6287C59474E6BC05D99B2964FA090C3A2233BA186515BE7ED1F612970CEE2D7AFB81BDD762170481CD0069127D5B05AA993B4EA988D8FDDC186FFB7DC90A6C08F4DF435C934063199FFFFFFFFFFFFFFFF  # pylint: disable=C0301
    base_g = 2

    def __init__(self):
        self.private_key = self._generate_primary_key()
        self.public_key = self._generate_public_key()

    def _generate_primary_key(self):
        return randint(2, self.modulus_p - 1)

    def _generate_public_key(self):
        return pow(self.base_g, self.private_key, self.modulus_p)

    def generate_key(self, another_key):
        """Generate a secret key based on the public key of another instance.

        :param another_key: The public key of another DiffieHellman instance.
        :type another_key: int
        :returns: A key only decipherable by the originator of 'another_key'.
        :rtype: str
        """
        shared_secret = pow(another_key, self.private_key, self.modulus_p)
        hashed_secret = hashlib.sha256()
        hashed_secret.update(str(shared_secret).encode())
        key = hashed_secret.digest()
        return hexlify(key)
