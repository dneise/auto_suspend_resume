#!/usr/bin/env python
from datetime import datetime, timedelta
import time
import pandas as pd
from fact.auxservices import MagicWeather
from db import scheduler
LIMIT = 50  # km/h
weather_sevice = MagicWeather()

measurement_types = pd.read_sql(
    "SELECT * FROM MeasurementTypes",
    scheduler
).set_index('fMeasurementTypeName').to_dict('index')
SUSPEND = measurement_types['Suspend']['fMeasurementTypeKey']
RESUME = measurement_types['Resume']['fMeasurementTypeKey']
STARTUP = measurement_types['Startup']['fMeasurementTypeKey']
SHUTDOWN = measurement_types['Shutdown']['fMeasurementTypeKey']


def main():
    should_currently_park = False

    while True:
        number_of_gusts_in_last_20_minutes = calculate_number_of_gusts()
        should_start = number_of_gusts_in_last_20_minutes > 2
        should_stop = number_of_gusts_in_last_20_minutes == 0

        should_currently_park = should_start or (
            should_currently_park and not should_stop)
        _is_suspended = is_suspended()

        if should_currently_park:
            if _is_suspended:
                # do nothing, we are already suspended.
                pass
            else:
                insert_row_into_schedule(type_key=SUSPEND)
        else:
            if _is_suspended:
                insert_row_into_schedule(type_key=RESUME)
            else:
                # do nothing, we should are already operating
                pass

        if _is_suspended and is_after_shutdown():
            # we should not be suspended after shutdown,
            # since in this case the shutdown is not executed
            # so we resume in order to perform the shutdown.
            insert_row_into_schedule(type_key=SUSPEND)

        time.sleep(30)  # seconds


def calculate_number_of_gusts():
    weather = read_some_files()
    weather.set_index('timestamp', inplace=True)
    weather.sort_index(inplace=True)

    weather['is_strong_gust'] = weather.wind_gust_speed > LIMIT
    now = datetime.utcnow()
    w = weather[now - timedelta(minutes=20):now]

    number_of_gusts_in_last_20_minutes = w.is_strong_gust.sum()
    return number_of_gusts_in_last_20_minutes


def read_some_files():
    return pd.concat([
        try_to_read_aux_file(datetime.today() + timedelta(days=-1)),
        try_to_read_aux_file(datetime.today() + timedelta(days=0)),
        try_to_read_aux_file(datetime.today() + timedelta(days=1)),
        ])


def try_to_read_aux_file(date_or_datetime):
    try:
        return weather_sevice.read_date(date_or_datetime)
    except:
        return pd.DataFrame()


def is_suspended():
    return fetch_last_entry_of_types_from_schedule(
            types=(SUSPEND, RESUME)
        ) == SUSPEND


def insert_row_into_schedule(
    type_key,
    date=datetime.utcnow(),
    db=scheduler
):
    db.engine.execute("""
    INSERT INTO Schedule
    (fStart, fMeasurementID, fUser, fMeasurementTypeKey)
    VALUES ('{date}', 0, "ASR", {type_key})
    """.format(
        date=date.isoformat(),
        type_key=type_key
        )
    )


def is_after_shutdown():
    return fetch_last_entry_of_types_from_schedule(
            types=(STARTUP, SHUTDOWN)
        ) == SHUTDOWN


def fetch_last_entry_of_types_from_schedule(types, engine=scheduler):
    '''select the last/current schedule entry-type from a set of types.

    Each row in our scheule has a type, e.g. Startup and Shutdown are types.
    Say we need to know if the telescope is current in operation or not.
    We say, we are currently in operation, when we are
        between Startup and Shutdown.
    And we are currently not in operation when we are
        between Shutdown and Startup.

    but this "between" condition is not really needed, all we need to know is
    if we are: "after a Startup" or "after a Shutdown" in order to understand
    if we are currently operating.

    In order to answer this question, we just filter all types out of the
    Schedule but "Startup" and "Shutdown" and look at *the last entry*.
    By *the last entry* we mean: Of all the entries with an "fStart" entry in
    the past, take the closest to *now*.

    The same applies to finding out if we are currently suspended or resumed.

    types: a tuple of Schedule.fMeasurementTypeKeys,
        e.g. (11, 12), i.e. (SUSPEND, RESUME)
    '''
    return pd.read_sql('''
        SELECT fMeasurementTypeKey FROM Schedule
        WHERE fMeasurementTypeKey in {types}
            AND fStart < '{now}'
        ORDER BY fStart DESC
        LIMIT 1
        '''.format(
            now=datetime.utcnow().isoformat(),
            types=types
        ),
        engine
    ).iloc[0].fMeasurementTypeKey


if __name__ == '__main__':
    main()
