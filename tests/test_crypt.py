from apium.crypt.dh import DiffieHellman
DiffieHellman.p = 0x7fffffff
DiffieHellman.g = 2

from apium.crypt import Encryptor


def test_diffie_hellman___generated_keys_are_the_same(port_num, running_worker):
    alice = DiffieHellman()
    bob = DiffieHellman()

    alice_key = alice.generate_key(bob.public_key)
    bob_key = bob.generate_key(alice.public_key)

    assert alice_key == bob_key


def test_message_encryption___message_could_be_decrypted(port_num, running_worker):
    server_enc = Encryptor()
    client_enc = Encryptor(server_enc._pk)

    msg = {'a': 'Allo', 'b': 2}
    encrypted_msg = client_enc.encrypt(msg)
    assert msg != encrypted_msg['data']

    decrypted_msg = server_enc.decrypt(encrypted_msg)
    assert msg == decrypted_msg


def test_message_encryption_when_libraries_not_installed___messages_assed_through(port_num, running_worker):
    server_enc = Encryptor()
    client_enc = Encryptor(server_enc._pk)
    client_enc._fern = 0
    client_enc._pk = None

    msg = {'a': 'Allo', 'b': 2}
    encrypted_msg = client_enc.encrypt(msg)
    assert msg == encrypted_msg['data']

    decrypted_msg = server_enc.decrypt(encrypted_msg)
    assert msg == decrypted_msg
