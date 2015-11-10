#!/usr/bin/env python
#
# This script returns HTTP GET requests to upload missing weather data
# to Wunderground
#
# Copyright 2014-2015 Adrian Weber
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ConfigParser import ConfigParser
import sqlite3
import sys
import urllib

def usage(argv):
    print "%s db starttime endtime" % argv[0]
    return 1

def main(argv=None):
    if argv is None:
        argv = sys.argv

    if len(argv) != 4:
        usage(argv)

    # Create a config parser and read the accompanying configuration file. The
    # configuration file needs to have the same name as the script but 
    # with .ini suffix
    config = ConfigParser()
    config.read("%sini" % argv[0].rstrip('py'))

    # See a full documentation of the protocol at
    # http://wiki.wunderground.com/index.php/PWS_-_Upload_Protocol
    wu_url = "http://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"

    # Start a new dict with default parameter "action" and "ID" and "PASSWORD"
    url_params = { "action": "updateraw",
        "ID": config.get("Wunderground", "id"),
        "PASSWORD": config.get("Wunderground", "password") }	

    # Establish a database connection
    db_connection = argv[1]
    conn = sqlite3.connect(db_connection)
	# Use a row factory to retrieve the fields easier
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Get the start and end time from the command line
    starttime = str(argv[2])
    endtime = str(argv[3])
    args = (starttime, endtime, )

    # Start the shell script output
    print "#!/bin/bash"

    query = """SELECT datetime(dateTime, \'unixepoch\', \'localtime\') AS localtime,
outTemp,
dewpoint,
barometer,
windDir,
windSpeed,
windGust,
outHumidity,
(
  SELECT SUM(a.rain)
  FROM archive AS a
  WHERE datetime(a.dateTime, 'unixepoch', 'localtime') > strftime('%Y-%m-%d %H:%M:%S', datetime(c.dateTime, 'unixepoch', 'localtime', '-1 hours'))
  AND datetime(a.dateTime, 'unixepoch', 'localtime') <= strftime('%Y-%m-%d %H:%M:%S', datetime(c.dateTime, 'unixepoch', 'localtime'))
) AS sum_last_hour,
(
  SELECT SUM(b.rain)
  FROM archive AS b
  WHERE datetime(b.dateTime, 'unixepoch', 'localtime') >= strftime('%Y-%m-%d %H:%M:%S', datetime(c.dateTime, 'unixepoch', 'localtime', 'start of day'))
  AND datetime(b.dateTime, 'unixepoch', 'localtime') <= strftime('%Y-%m-%d %H:%M:%S', datetime(c.dateTime, 'unixepoch', 'localtime'))
) AS sum_day,
datetime(dateTime, \'unixepoch\') AS utctime
FROM archive AS c
WHERE datetime(dateTime, \'unixepoch\', \'localtime\') >= strftime(\'%Y-%m-%d %H:%M:%S\', ?)
AND datetime(dateTime, \'unixepoch\', \'localtime\') <= strftime(\'%Y-%m-%d %H:%M:%S\', ?)"""

    for row in c.execute(query, args):

        if row['utctime'] is not None:
            url_params['dateutc'] = urllib.quote(row['utctime'])
        if row['outTemp'] is not None:
            url_params['tempf'] = "%.1f" % row['outTemp']
        if row['dewpoint'] is not None:
            url_params['dewptf'] = "%.1f" % row['dewpoint']
        if row['barometer'] is not None:
            url_params['baromin'] = "%.3f" % row['barometer']
        if row['windDir'] is not None:
            url_params['winddir'] = "%03.0f" % row['windDir']
        if row['windSpeed'] is not None:
            url_params['windspeedmph'] = "%.1f" % row['windSpeed']
        if row['windGust'] is not None:
            url_params['windgustmph'] = "%.1f" % row['windGust']
        if row['outHumidity'] is not None:
            url_params['humidity'] = "%03.0f" % row['outHumidity']
        # Calulate parameter "rainin" which is the accumulated rainfall in the
        # past 60 minutes in inch
        if row['sum_last_hour'] is not None:
            url_params['rainin'] = "%.2f" % row['sum_last_hour']
        # Parameter "dailyrainin" is the rain in inches so far for this day in
        # local time. For such purposes Sqlite provides the handy modifier
        # "start of day", see also the documentation at 
        # https://www.sqlite.org/lang_datefunc.html
        # Beware: although unreasonable the timestamp 00:00:00 in UTC (i.e.
        # 17:00:00 in ITC) belongs to the next day, see also comments
        # in class RESTThread in file restx.py.
        if row['sum_day'] is not None:
            url_params['dailyrainin'] = "%.2f" % row['sum_day']

        # Add additionally the used software type
        url_params['softwaretype'] = urllib.quote("Custom script")
        urlquery = '&'.join(["%s=%s" % (k, v) for k, v in url_params.items()])
        print "wget -O - \"%s?%s\"" % (wu_url, urlquery)
        # CSV output for debugging purposes
        #print ','.join(["%s" % (v) for k, v in url_params.items()])

    # Finally close the connection
    conn.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
