import os
import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from ..config import get_settings
from .validators import validate_email

settings = get_settings()


class User(Model):
    __keyspace__ = os.getenv('ASTRADB_KEYSPACE')
    email = columns.Text(primary_key=True)
    user_id = columns.UUID(primary_key=True, default=uuid.uuid1)
    password = columns.Text()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'User(email={self.email}, user_id={self.user_id})'

    @staticmethod
    def create_user(email, password=None):
        if User.objects.filter(email=email).count():
            raise Exception('User already has an account')
        valid, msg, email = validate_email(email=email)
        if not valid:
            raise Exception(f'Invalid email: {msg}')
        obj = User(email=email, password=password)
        obj.save()
        return obj
