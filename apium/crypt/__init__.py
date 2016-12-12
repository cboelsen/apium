# -*- coding: utf-8 -*-

"""
.. module: crypt
    :synopsis: Message encryption and decryption.
"""


__all__ = ('Encryptor', )


import base64
import pickle

from .dh import DiffieHellman


class NoEncryptionLibrariesInstalled(Exception):
    """Raised if an encrypted message is received, but there are no libraries
    available to decrypt the message.
    """
    pass


class Encryptor:
    """Handles the encryption and decryption of messages.

    Note that this requires the 'cryptography' package to be installed,
    otherwise this class will default back to unencrypted messages.

    Here's a brief example of how to use the Encryptors:

    .. doctest::

        >>> from apium.crypt import Encryptor
        >>> server_enc = Encryptor()
        >>> client_enc = Encryptor(server_enc._pk)
        >>> msg = {'a': 'Allo', 'b': 2}
        >>> encrypted_msg = client_enc.encrypt(msg)
        >>> msg != encrypted_msg['data']
        True
        >>> decrypted_msg = server_enc.decrypt(encrypted_msg)
        >>> msg == decrypted_msg
        True

    :param remote_pk: The public key from another Encryptor instance.
    :type remote_pk: int
    """

    def __init__(self, remote_pk=None):
        self._fern = None
        self._dh = DiffieHellman()
        self._pk = self._dh.public_key
        self._remote_pk = remote_pk

    def _fernet(self):
        if self._fern is None:
            try:
                from cryptography.fernet import Fernet
                key = self._dh.generate_key(self._remote_pk)
                self._fern = Fernet(base64.urlsafe_b64encode(key[:32]))
            except ImportError:
                self._pk = None
                self._fern = 0
        return self._fern

    def encrypt(self, msg):
        """Return a dict containing the (pickled then) encrypted python object.

        Note that this method requires access to the public key of the Encryptor
        that will be decrypting this message, either by having received a
        message from it previously, or by explicitly passing it in during
        construction.

        :param msg: A pickleable python object.
        :type msg: object
        :returns: The encrypted message format to pass to another Encryptor.
        :rtype: dict
        """
        return {
            'data': self._fernet().encrypt(pickle.dumps(msg)) if self._fernet() else msg,
            'pk': self._pk,
        }

    def decrypt(self, msg):
        """Return the decrypted message.

        :param msg: A dict returned by encrypt()
        :type msg: dict
        :returns: The original unencrypted message from the originating
            Encryptor.
        :rtype: object
        """
        remote_pk = msg['pk']
        if remote_pk:
            self._remote_pk = remote_pk
            if not self._fernet():
                raise NoEncryptionLibrariesInstalled('"cryptography" not installed - cannot decrypt message')
            return pickle.loads(self._fernet().decrypt(msg['data']))
        else:
            return msg['data']
