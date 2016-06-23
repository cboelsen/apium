import base64
import pickle

from .dh import DiffieHellman


class NoEncryptionLibrariesInstalled(Exception):
    pass


class Encryptor:

    def __init__(self, remote_pk=None):
        self._fern = None
        self._dh = DiffieHellman()
        self._pk = self._dh.public_key
        self._remote_pk = remote_pk

    def _fernet(self):
        if self._fern is None:
            try:
                from cryptography.fernet import Fernet
                self._dh.generate_key(self._remote_pk)
                self._fern = Fernet(base64.urlsafe_b64encode(self._dh.get_key()[:32]))
            except ImportError:
                self._pk = None
                self._fern = 0
        return self._fern

    def encrypt(self, msg):
        return {
            'data': self._fernet().encrypt(pickle.dumps(msg)) if self._fernet() else msg,
            'pk': self._pk,
        }

    def decrypt(self, msg):
        remote_pk = msg['pk']
        if remote_pk:
            self._remote_pk = remote_pk
            if not self._fernet():
                raise NoEncryptionLibrariesInstalled('"cryptography" not installed - cannot decrypt message')
            return pickle.loads(self._fernet().decrypt(msg['data']))
        else:
            return msg['data']
