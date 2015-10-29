#!/usr/bin/env python
#
#

from datetime import datetime, timedelta, tzinfo
import sqlite3
import sys
import urllib

class UTC(tzinfo):
    def dst(self, dt):
        return timedelta(hours=0)
    def utcoffset(self, dt):
        return timedelta(hours=0)

class ITC(tzinfo):
    def dst(self, dt):
        return timedelta(hours=0)
    def utcoffset(self, dt):
        return timedelta(hours=7)

def usage(argv):
    print "%s db starttime endtime" % argv[0]
    sys.exit(0)

def main(argv=None):
    if argv is None:
        argv = sys.argv

    if len(argv) != 4:
        usage(argv)

    wu_url = "http://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"

    url_params = { "action": "updateraw",
        "ID": "IVIENTIA5",
        "PASSWORD": "rh2N_6rRIXr-eb" }	

    db_connection = argv[1]
    conn = sqlite3.connect(db_connection)
    c = conn.cursor()

    # Timezone difference in hours from utc
    tzone = "ITC"

    # Get the start and end time from the command line
    starttime = str(argv[2])
    endtime = str(argv[3])
    args = (starttime, endtime, )

    print "#!/bin/bash"

    for row in c.execute('SELECT datetime(dateTime, \'unixepoch\', \'localtime\'), outTemp, dewpoint, barometer, windDir, windSpeed, windGust, outHumidity FROM archive WHERE datetime(dateTime, \'unixepoch\') >= strftime(\'%Y-%m-%d %H:%M:%S\', ?) AND datetime(dateTime, \'unixepoch\') <= strftime(\'%Y-%m-%d %H:%M:%S\', ?)', args):

        if row[0] is not None:
            # Convert first the datetime from local time to utc
            dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=ITC())
            # print dt.astimezone(UTC())
            url_params['dateutc'] = urllib.quote(dt.astimezone(UTC()).strftime("%Y-%m-%d %H:%M:%S"))
        if row[1] is not None:
            url_params['tempf'] = "%.1f" % row[1]
        if row[2] is not None:
            url_params['dewptf'] = "%.1f" % row[2]
        if row[3] is not None:
            url_params['baromin'] = "%.3f" % row[3]
        if row[4] is not None:
            url_params['winddir'] = "%03.0f" % row[4]
        if row[5] is not None:
            url_params['windspeedmph'] = "%.1f" % row[5]
        if row[6] is not None:
            url_params['windgustmph'] = "%.1f" % row[6]
        if row[7] is not None:
            url_params['humidity'] = "%03.0f" % row[7]
        # Calculate the rain from the last hour
        # print "SELECT datetime(dateTime, 'unixepoch', 'localtime'), rain FROM archive WHERE datetime(dateTime, 'unixepoch', 'localtime') <= strftime('%%Y-%%m-%%d %%H:%%M:%%S', '%s') AND datetime(dateTime, 'unixepoch', 'localtime') > strftime('%%Y-%%m-%%d %%H:%%M:%%S', '%s', '-1 hours');" % (row[0], row[0])

        # As arguments we use the timestamp from the current record
        rainArgs = (row[0], row[0],)

        # Start a new cursor
        d = conn.cursor()
        d.execute('SELECT SUM(rain) FROM archive WHERE datetime(dateTime, \'unixepoch\', \'localtime\') <= strftime(\'%Y-%m-%d %H:%M:%S\', ?) AND datetime(dateTime, \'unixepoch\', \'localtime\') > strftime(\'%Y-%m-%d %H:%M:%S\', ?, \'-1 hours\')', rainArgs)
        rainin = d.fetchone()[0]
        if rainin is not None:
            url_params['rainin'] = "%.2f" % rainin

        # Calculate the rain from the last 24 hour 
        d.execute('SELECT SUM(rain) FROM archive WHERE datetime(dateTime, \'unixepoch\', \'localtime\') <= strftime(\'%Y-%m-%d %H:%M:%S\', ?) AND datetime(dateTime, \'unixepoch\', \'localtime\') > strftime(\'%Y-%m-%d %H:%M:%S\', ?, \'-24 hours\')', rainArgs)
        dailyrainin = d.fetchone()[0]
        if dailyrainin is not None:
            url_params['dailyrainin'] = "%.2f" % dailyrainin

        # Add additionally the used software type
        url_params['softwaretype'] = urllib.quote("Custom script")
        urlquery = '&'.join(["%s=%s" % (k, v) for k, v in url_params.items()])
        print "wget -O - \"%s?%s\"" % (wu_url, urlquery)

    # Finally close the connection
    conn.close()

if __name__ == "__main__":
    sys.exit(main())
