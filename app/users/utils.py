from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

def generate_hash(password: str) -> str:
    """Generate hash for a given password."""
    hasher = PasswordHasher()
    return hasher.hash(password=password)


def check_hash(password_hash: str, password_raw: str) -> tuple[bool, str]:
    hasher = PasswordHasher()
    correct = False
    msg = ''
    try:
        correct = hasher.verify(hash=password_hash, password=password_raw)
    except VerifyMismatchError:
        msg = 'Invalid password!'
    except Exception as e:
        msg = f'Error during checking password hash: {e}'
    return correct, msg
