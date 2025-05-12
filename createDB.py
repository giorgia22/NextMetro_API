from gtfsdb import GTFS, Database
from sqlalchemy import *

gtfs = GTFS("https://dati.comune.milano.it/gtfs.zip")
engine = create_engine("sqlite+pysqlite:///test_db.db", echo=True)

db = Database(url = engine.url)
db.create()
gtfs.load(db)