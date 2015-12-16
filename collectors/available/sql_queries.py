#!/usr/bin/python

# We need to following modules installed for this collector to run
# sudo {apt-get | yum| install libpq-dev
# sudo pip install psycopg2

import sys
import time
import logging

try:
    import psycopg2
except ImportError:
    psycopg2 = None # handle gracefully, but dump and complain about missing import

try:
    import pymysql
except ImportError:
    pymysql = None # This is handled gracefully in main()

from collectors.etc import sql_queries_conf
from collectors.lib import utils

settings = sql_queries_conf.get_settings()

logFileName = settings['logfile']
logLevelName = settings['loglevel']
logLevel = getattr(logging, logLevelName.upper())
if not isinstance(logLevel, int):
    raise ValueError('Invalid log level: %s' % (logLevelName))
logging.basicConfig(filename=logFileName, level=logLevel)

stats = {}
stats['received'] = 0
stats['errors'] = 0
stats['sent'] = 0

rowCount = 0

def getColumnNames(cursor):
    return [i[0] for i in cursor.description]

def doQueries(conn, db):
    for query in db['queries']:
        cur = conn.cursor()
        sys.stderr.write('Executing: %s\n' % query['sql'])
        cur.execute(query['sql'])
        names = getColumnNames(cur)
        for row in cur:
            stats['received'] += 1
            fields = dict(zip(names, row))
            metric = None
            try:
                metric = query['metric'].format(**fields)
                value = query['value'].format(**fields)
                tags = {};
                if 'tags' in query:
                    for (tk, tv) in query['tags'].items():
                        tk = tk.format(**fields)
                        tv = tv.format(**fields)
                        tags[tk] = tv
            except KeyError as e:
                stats['errors'] += 1
                sys.stderr.write('KeyError %s: %s\n' % (e, fields))

            if (metric):
                print "%s %d %s" % (metric, time.time(), value),
                for (tagk, tagv) in tags.items():
                    if (tagv is not None) and (tagv != ''):
                        # print "%s=%s" % (camelCase(tagk), re.sub(disallow, '', tagv)),
                        print "%s=%s" % (tagk, tagv),
                print ""
                stats['sent'] += 1
                rowCount += 1
        cur.close()

    return lineCount


def main():
    utils.drop_privileges()
    if psycopg2 is None:
        utils.err("error: Python module `psycopg2' is missing. PostgreSQL queries won't be executed.")
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing. MySQL queries won't be executed.")

    startTime = time.time()

    for db in settings['databases']:
        conn = None
        if db['driver'] == 'postgresql':
            if (psycopg2):
                conn = psycopg2.connect(**db['settings'])
        elif db['driver'] == 'mysql':
            if (pymsql):
                conn = pymsql.connect(**db['settings'])
        if (conn):
            doQueries(conn, db)
            conn.close()

    # write zabbix_collector metric to opentsdb
    if (time.time() - startTime) > 5:
        itemsPerSec = rowCount / (time.time() - startTime)
        print "tcollector.items.received %d %f collector=sql_queries host=%s\n" % (time.time(), stats['received'], socket.getfqdn())
        print "tcollector.items.errors %d %f collector=sql_queries host=%s\n" % (time.time(), stats['errors'], socket.getfqdn())
        print "tcollector.items.sent %d %f collector=sql_queries host=%s\n" % (time.time(), stats['sent'], socket.getfqdn())
        print "tcollector.items.perSecond %d %f collector=sql_queries host=%s\n" % (time.time(), itemsPerSec, socket.getfqdn())
    sys.stdout.flush()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())


