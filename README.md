# auto_suspend_resume

## "Install"

this is a single script application, so no real installation.
Just `git clone` the repo into some folder. Specify the DB credentials and execute the
script, like:

    python asr.py

It runs forever (unless there is a bug). Still you might want to add some kind
of restart line into your crontab like:

    */5 * * * * if ! ps aux | grep asr.py | grep -v grep >/dev/null 2>&1; then some/path/asr.py; fi


### `db.py` needed

For this to run, you'll need to rename the `db.py.template` to `db.py`
and enter a valid username password combination, with the necessary access rights.

