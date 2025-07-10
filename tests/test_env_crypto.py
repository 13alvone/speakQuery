import os
import sys
from pathlib import Path
from cryptography.fernet import Fernet

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.env_crypto import encrypt_env, decrypt_env


def test_encrypt_decrypt(tmp_path, monkeypatch):
    key = Fernet.generate_key()
    monkeypatch.setenv("MASTER_KEY", key.decode())
    plain = tmp_path / ".env"
    enc = tmp_path / ".env.enc"
    plain.write_text("TEST_KEY=VALUE\n")

    encrypt_env(str(plain), str(enc))
    assert enc.exists()
    decrypted = decrypt_env(str(enc))
    assert "TEST_KEY=VALUE" in decrypted

