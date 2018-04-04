# auto_suspend_resume

## "Install"

### Dependencies

    pip install json_log_formatter

this is a single script application, so no real installation.
Just `git clone` the repo into some folder. Specify the DB credentials and execute the
script, like:

    $ python asr.py

On newdaq this script is (re-)started once per hour by cron (in case it died):

    28 * * * * if ! ps aux | grep asr.py | grep -v grep >/dev/null 2>&1; then /home/fact/anaconda3/bin/python /home/fact/auto_suspend_resume/asr.py; fi

Experts or shifters who need to shut it down should:

 * comment out the line above from the crontab
 * `sudo killall asr.py`


### `db.py` needed

For this to run, you'll need to rename the `db.py.template` to `db.py`
and enter a valid username password combination, with the necessary access rights.

