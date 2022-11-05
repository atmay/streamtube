from fastapi import FastAPI
from cassandra.cqlengine.management import sync_table

from app.config import get_settings
from app.users.models import User
from app.db import get_db_session

app = FastAPI()
DB_SESSION = None
settings = get_settings()


@app.on_event('startup')
def on_startup():
    global DB_SESSION
    DB_SESSION = get_db_session()
    sync_table(User)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/users")
async def users_list_view():
    users = User.objects.all()
    return {'users': users}
