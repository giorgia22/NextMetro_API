import uvicorn

from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import Session
from gtfsdb import StopTime, Trip, UniversalCalendar
import datetime
import logging
from fastapi import FastAPI
import datetime 

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
    date = date.astimezone()
    queryTime = ""

    if date.hour < 4:
        date -= datetime.timedelta(days=1)
        queryTime = f'{date.hour + 24}:{date.astimezone().strftime("%M:%S")}'

    else:
        queryTime = date.strftime("%H:%M:%S")
        
    queryDate = date.strftime("%Y-%m-%d")

    # DB query with SQLAlchemy, it needs to open a session to the DB
    with Session(engine) as session:
        result = (
            session.query(StopTime.stop_id, StopTime.arrival_time, StopTime.trip_id, StopTime.stop_headsign)
            .join(Trip, StopTime.trip)
            .join(UniversalCalendar, Trip.universal_calendar)
            .filter(
                UniversalCalendar.date == queryDate,
                StopTime.stop_id == stopId,
                StopTime.arrival_time > queryTime,
            )
            .order_by(StopTime.arrival_time)
            .all()
        )

        t = 0
        out = list(())

        for row in result:
            if headsign in row.stop_headsign:
                arrivalDate = date.date()
                hour, min, sec = map(int, row.arrival_time.split(':'))

                logger.debug(f"return: {hour}:{min}:{sec}")
                
                if(hour >= 24):
                    arrivalDate += datetime.timedelta(days=1)
                    hour -= 24
               
                arrivalTime = datetime.time(hour, min, sec)
                arrivalDatetime = datetime.datetime.combine(arrivalDate, arrivalTime)

                out.append({
                    "stopId": row.stop_id,
                    "tripId": row.trip_id,
                    "headsign": row.stop_headsign,
                    "arrivalTime": arrivalDatetime,
                })
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


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=30000) #select on which port to run it
"""
TODO:
- get nearest stop_id
- search stop_ids
- show stops map
- api docs
- better code
"""
