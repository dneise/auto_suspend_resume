#!/usr/bin/env python
from datetime import datetime, timedelta
import time
import pandas as pd
from fact.auxservices import MagicWeather
from db import scheduler
LIMIT = 50  # km/h
weather_sevice = MagicWeather()


def main():
    should_currently_park = False

    while True:
        time.sleep(30)  # seconds

        g_20 = calc_g_20()
        should_start = g_20 > 2
        should_stop = g_20 <= 0

        should_currently_park = should_start or (
            should_currently_park and not should_stop)
        _is_suspended = is_suspended()

        if should_currently_park:
            if _is_suspended:
                # do nothing, we are already suspended.
                pass
            else:
                make_suspend_entry()
        else:
            if _is_suspended:
                make_resume_entry()
            else:
                # do nothing, we should are already operating
                pass

        if _is_suspended and is_after_shutdown():
            # we should not be suspended after shutdown,
            # since in this case the shutdown is not executed
            # so we resume in order to perform the shutdown.
            make_resume_entry()


def calc_g_20():
    weather = read_some_files()
    weather.set_index('timestamp', inplace=True)
    weather.sort_index(inplace=True)

    weather['is_strong_gust'] = weather.wind_gust_speed > LIMIT
    now = datetime.utcnow()
    w = weather[now - timedelta(minutes=20):now]

    g_20 = w.is_strong_gust.sum()
    return g_20


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
    '''
    fMeasurementTypeKey:
     11 --> Suspend
     12 --> Resume
    '''
    last_suspend_or_resume_entry = pd.sql('''
        SELECT fMeasurementTypeKey FROM Schedule
        WHERE fMeasurementTypeKey in (11, 12)
            AND fStart < '{}'
        ORDER BY fStart DESC
        LIMIT 1
    '''.format(datetime.utcnow().isoformat())
    )
    return last_suspend_or_resume_entry.iloc[0].fMeasurementTypeKey == 11


def make_suspend_entry():
    print("I would make a suspend entry now", datetime.utcnow())
    # insert_suspend_at(datetime.utcnow())


def insert_suspend_at(date, db=scheduler):
    scheduler.engine.execute("""
    INSERT INTO Schedule
    (fStart, fMeasurementID, fUser, fMeasurementTypeKey)
    VALUES ('{}', 0, "ASR", 11)
    """.format(date.isoformat())
    )


def make_resume_entry():
    print("I would make a resume entry now", datetime.utcnow())
    # insert_resume_at(datetime.utcnow())


def insert_resume_at(date, db=scheduler):
    scheduler.engine.execute("""
    INSERT INTO Schedule
    (fStart, fMeasurementID, fUser, fMeasurementTypeKey)
    VALUES ('{}', 0, "ASR", 12)
    """.format(date.isoformat()))


def delete_row(id, db=scheduler):
    scheduler.engine.execute("""
    DELETE FROM Schedule where fScheduleID = {}""".format(id))


def is_after_shutdown():
    '''
    fMeasurementTypeKey:
     0 --> Startup
     6 --> Shutdown
    '''
    last_startup_or_shutdown = pd.sql('''
        SELECT fMeasurementTypeKey FROM Schedule
        WHERE fMeasurementTypeKey in (0, 6)
            AND fStart < '{}'
        ORDER BY fStart DESC
        LIMIT 1
    '''.format(datetime.utcnow().isoformat())
    )
    return last_startup_or_shutdown.iloc[0].fMeasurementTypeKey == 6


if __name__ == '__main__':
    main()
