from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from gtfsdb import StopTime, Trip, CalendarDate
import datetime
from fastapi import FastAPI

app = FastAPI()
DbUrl = "sqlite+pysqlite:///test_db.db"
engine = create_engine(DbUrl)


@app.get("/")
def getNextArrivals():
    # def getNextArrivals(stopId, fromDatetime, headsign="", count=1):
    stopId = "VIMODRONE"
    fromDatetime = datetime.datetime.now()
    headsign = ""
    count = 4

    with Session(engine) as session:
        result = (
            session.query(StopTime)
            .join(Trip, StopTime.trip_id == Trip.trip_id)
            .join(CalendarDate, Trip.service_id == CalendarDate.service_id)
            .filter(
                CalendarDate.date == fromDatetime.strftime("%Y-%m-%d"),
                StopTime.stop_id == stopId,
                StopTime.arrival_time > fromDatetime.strftime("%H:%M:%S"),
            )
            .order_by(StopTime.arrival_time)
            .all()
        )

        t = 0
        print("Next metro at Vimodrone for gessate: ")

        out = list(())
        for row in result:
            if headsign in row.stop_headsign:
                out.append(row)
                t += 1
            if t >= count:
                break
        return out


# print(getNextArrivals("VIMODRONE", datetime.datetime.now(), "", count=4))

"""
TODO:
- get nearest stop_id
- search stop_ids
- show stops map
"""
