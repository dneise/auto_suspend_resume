# auto_suspend_resume

## "Install"

### Dependencies

    pip install json_log_formatter

this is a single script application, so no real installation.
Just `git clone` the repo into some folder. Specify the DB credentials and execute the
script, like:

    python asr.py

It runs forever (unless there is a bug). Still you might want to add some kind
of restart line into your crontab like

    15 12 * * * if ! ps aux | grep asr.py | grep -v grep >/dev/null 2>&1; then some/path/asr.py; fi

In order to restart it on a daily basis, to make sure it is running after a power cut or so.

### `db.py` needed

For this to run, you'll need to rename the `db.py.template` to `db.py`
and enter a valid username password combination, with the necessary access rights.

