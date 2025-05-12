import uvicorn

from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import Session
from gtfsdb import StopTime, Trip, UniversalCalendar
import datetime
import logging
from fastapi import FastAPI

app = FastAPI()
logger = logging.getLogger(__name__)

# create with SQLAlchemy an engine to access the DB, with three / it's a local path
DbUrl = "sqlite:///test_db.db"
engine = create_engine(DbUrl)


@app.get("/next_arrivals/{stopId}/{date}")
def getNextArrivals(
    stopId: str, date: datetime.datetime, headsign: str = "", count: int = None
):
    logger.debug("getNextArrivals()")

    # DB query with SQLAlchemy, it needs to open a session to the DB
    with Session(engine) as session:
        result = (
            session.query(StopTime.stop_id, StopTime.arrival_time, StopTime.trip_id, StopTime.stop_headsign)
            .join(Trip, StopTime.trip)
            .join(UniversalCalendar, Trip.universal_calendar)
            .filter(
                UniversalCalendar.date == date.astimezone().strftime("%Y-%m-%d"),
                StopTime.stop_id == stopId,
                StopTime.arrival_time > date.astimezone().strftime("%H:%M:%S"),
            )
            .order_by(StopTime.arrival_time)
            .all()
        )

        t = 0
        out = list(())

        for row in result:
            if headsign in row.stop_headsign:
                out.append(row)
                t += 1
            if count is not None and t >= count:
                break

        logger.info(
            f"Get next {count} metro at {stopId} {'for' if headsign != '' else ''} {headsign}"
        )
        return out


@app.get("/stops")
def getAllStops(metroLine: str = None):
    logger.debug("getAllStops()")

    with Session(engine) as session:
        result = (
            session.query(StopTime.stop_id, Trip.route_id)
            .join(Trip, StopTime.trip)
            .filter(Trip.route_id.like("M%" if metroLine is None else metroLine))
            .distinct()
        )
    return result


"""
TODO:
- get nearest stop_id
- search stop_ids
- show stops map
- api docs
- better code
"""
