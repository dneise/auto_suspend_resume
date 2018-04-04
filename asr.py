#!/usr/bin/env python
from datetime import datetime, timedelta
import time
import pandas as pd
from fact.auxservices import MagicWeather
import json
from db import scheduler
from asr_logging import logging, logger


logger.setLevel(logging.INFO)
LIMIT = 50  # km/h
RECENT_PAST = timedelta(minutes=20)

weather_sevice = MagicWeather()

measurement_types = pd.read_sql(
    "SELECT * FROM MeasurementType",
    scheduler
).set_index('fMeasurementTypeName').to_dict('index')
SUSPEND = measurement_types['Suspend']['fMeasurementTypeKey']
RESUME = measurement_types['Resume']['fMeasurementTypeKey']
STARTUP = measurement_types['Startup']['fMeasurementTypeKey']
SHUTDOWN = measurement_types['Shutdown']['fMeasurementTypeKey']


def main():
    should_currently_park = False
    logger.info('starting up')

    while True:
        number_of_gusts_in_recent_past = calculate_number_of_gusts()
        start_parking = number_of_gusts_in_recent_past > 2
        stop_parking = number_of_gusts_in_recent_past == 0

        should_currently_park = start_parking or (
            should_currently_park and not stop_parking
        )
        _is_suspended = is_suspended()

        if should_currently_park and not _is_suspended:
            logger.info('suspending operation')
            insert_into_schedule(type_key=SUSPEND)

        if not should_currently_park and _is_suspended:
            logger.info('resuming operation')
            insert_into_schedule(type_key=RESUME)

        if _is_suspended and is_after_shutdown():
            # we should not be suspended after shutdown,
            # since in this case the shutdown is not executed
            # so we resume in order to perform the shutdown.
            insert_into_schedule(type_key=SUSPEND)

        output_current_status_json({
            'number_of_gusts_in_recent_past': number_of_gusts_in_recent_past,
            'should_currently_park': should_currently_park,
            '_is_suspended': _is_suspended,
            'is_after_shutdown': is_after_shutdown(),
        })
        time.sleep(30)  # seconds


def calculate_number_of_gusts():
    weather = read_some_files()
    weather.set_index('timestamp', inplace=True)
    weather.sort_index(inplace=True)
    weather = weather[['wind_gust_speed']]
    weather = weather.dropna()

    now = datetime.utcnow()
    weather = weather[now - RECENT_PAST:now]
    # these values are written about once per 30sec
    # so weather should have ~40 entries.
    # let's log when there are less than 35
    if len(weather) < 35:
        logger.info(
            'weather has less than 35 entires',
            extra={
                'len(weather)': len(weather)
            }
        )

    weather['is_strong_gust'] = weather.wind_gust_speed > LIMIT

    logger.debug(
        transform_dataframe_to_dict_for_json_log(weather)
    )
    number_of_gusts_in_recent_past = weather.is_strong_gust.sum()
    return number_of_gusts_in_recent_past


def read_some_files():

    def try_to_read_aux_file(date_or_datetime):
        try:
            return weather_sevice.read_date(date_or_datetime)
        except:
            return pd.DataFrame()

    return pd.concat([
        try_to_read_aux_file(datetime.today() + timedelta(days=-1)),
        try_to_read_aux_file(datetime.today() + timedelta(days=0)),
        try_to_read_aux_file(datetime.today() + timedelta(days=1)),
        ])


def insert_into_schedule(
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


def is_suspended():
    return select_last_type_from_schedule_set_of_types(
            types=(SUSPEND, RESUME)
        ) == SUSPEND


def is_after_shutdown():
    return select_last_type_from_schedule_set_of_types(
            types=(STARTUP, SHUTDOWN)
        ) == SHUTDOWN


def select_last_type_from_schedule_set_of_types(types, engine=scheduler):
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


def transform_dataframe_to_dict_for_json_log(df):
    s = df.reset_index(drop=False).to_json(
        orient='records',
        date_format='iso'
    )
    return json.loads(s)


def output_current_status_json(status):
    if 'timestamp' not in status:
        status['timestamp'] = datetime.utcnow().isoformat()

    with open('current_status.json', 'w') as file:
        file.write(json.dumps(status))


if __name__ == '__main__':
    main()
