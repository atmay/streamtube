import os
import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from ..config import get_settings
from .validators import validate_email
from .utils import generate_hash, check_hash

settings = get_settings()


class User(Model):
    __keyspace__ = os.getenv('ASTRADB_KEYSPACE')
    email = columns.Text(primary_key=True)
    user_id = columns.UUID(primary_key=True, default=uuid.uuid1)
    password = columns.Text()

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f'User(email={self.email}, user_id={self.user_id})'

    @staticmethod
    def create_user(email: str, password: str = None):
        if User.objects.filter(email=email).count():
            raise Exception('User already has an account')
        valid, msg, email = validate_email(email=email)
        if not valid:
            raise Exception(f'Invalid email: {msg}')
        obj = User(email=email, password=password)
        obj.set_password(password)
        obj.save()
        return obj

    def set_password(self, password: str, commit: bool = False) -> None:
        password_hashed = generate_hash(password)
        self.password = password_hashed
        if commit:
            self.save()

    def check_password(self, password: str) -> bool:
        checked, _ = check_hash(password_hash=self.password, password_raw=password)
        return checked
