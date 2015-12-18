#!/usr/bin/env python
#
# Copyright (C) 2014  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
# Dump all replication item/metric insert events from a zabbix mysql server
#

import regex as re
import csv
import sys
import traceback
from collections import OrderedDict
import simplejson as json
import time
import logging
import socket
import sqlite3
import random
import cProfile

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        WriteRowsEvent
    )
except ImportError:
    BinLogStreamReader = None  # This is handled gracefully in main()

try:
    import pymysql
except ImportError:
    pymysql = None # This is handled gracefully in main()

from collectors.etc import zabbix_bridge_conf
from collectors.lib import utils

settings = zabbix_bridge_conf.get_settings()
disallow = re.compile(settings['disallow'])
csv.register_dialect('zabbix1', escapechar='\\', doublequote=False)

logFileName = settings['logfile']
logLevelName = settings['loglevel']
logLevel = getattr(logging, logLevelName.upper())
if not isinstance(logLevel, int):
    raise ValueError('Invalid log level: %s' % (logLevelName))
logging.basicConfig(filename=logFileName, level=logLevel) #, format='%(asctime)-15s %(clientip)s %(user)-8s %(message)s')

stats = {}
stats['received'] = 0
stats['updated'] = 0
stats['errors'] = 0
stats['sent'] = 0
stats['itemsWrittenToCache'] = 0
stats['itemsReadFromCache'] = 0
stats['itemsExpired'] = 0
stats['macrosWrittenToCache'] = 0
stats['macrosReadFromCache'] = 0
stats['macrosExpired'] = 0
stats['avgItemRefreshTime'] = 0
stats['avgMacroRefreshTime'] = 0
stats['rowCount'] = 0
stats['rowsSkipped'] = 0
stats['fetchCount'] = 0
stats['fetchTime'] = 0
stats['buildCount'] = 0
stats['buildTime'] = 0

transTagKeys = {
    '^\"(.*)\"$': '\\1', # strip leading/trailing quotes
    '\s([A-Z])': '\\1',  # strip spaces followed by a capital letter (Camel Case => CamelCase)
    '\s([a-z])': '_\\1', # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
    '\s([0-9])': '.\\1', # replace spaces followed by a number with a dot (item 1 => item.1)
    '^([A-Z])': lambda p: p.group(1).lower(), # lower-case the first letter of the tag key, if its not
    settings['disallow']: ''
}
transTagKeysRegex = re.compile("(%s)" % "|".join(map(re.escape, transTagKeys.keys())))

transTagVals = {
    '^\"(.*)\"$': '\\1', # strip leading/trailing quotes
    '\s([A-Z])': '\\1',  # strip spaces followed by a capital letter (Camel Case => CamelCase)
    '\s([a-z])': '_\\1', # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
    '\s([0-9])': '.\\1', # replace spaces followed by a number with a dot (item 1 => item.1)
    settings['disallow']: ''
}
transTagValsRegex = re.compile("(%s)" % "|".join(map(re.escape, transTagVals.keys())))

transMetric = {
    '^\"(.*)\"$': '\\1', # strip leading/trailing quotes
    '\s([A-Z])': '\\1',  # strip spaces followed by a capital letter (Camel Case => CamelCase)
    '\s([a-z])': '_\\1', # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
    '\s([0-9])': '.\\1', # replace spaces followed by a number with a dot (item 1 => item.1)
    #metric = re.sub('^([A-Z])', lambda p: p.group(1).lower(), metric) # lower-case the first letter of the metric, if its not
    settings['disallow']: '' # final scrub for invalid characters (strip them)
}
transMetricRegex = re.compile("(%s)" % "|".join(map(re.escape, transMetric.keys())))

def configCache(mapDb):
    c = mapDb.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS macros (host TEXT, key TEXT, value TEXT, nextUpdate INTEGER, PRIMARY KEY(host, key, value))')
    c.execute('CREATE TABLE IF NOT EXISTS items  (itemId INTEGER PRIMARY KEY, host TEXT, key TEXT, metric TEXT, nextUpdate INTEGER, UNIQUE(itemId, host, key))')
    c.execute('CREATE TABLE IF NOT EXISTS tags   (itemId INTEGER, tagk TEXT, tagv TEXT, UNIQUE(itemId, tagk, tagv))')
    logging.debug("Created (if not exists) zabbix mapping schema...")
    mapDb.commit()

def writeMacros(mapDb, macros):
    c = mapDb.cursor()
    for (host, macro) in macros.items():
        logging.debug("storeMacros: Writing macros to cache for host %s" % (host))
        for (k, v) in macro.items():
            try:
                if not host:
                    host = '__global__'
                c.execute('''INSERT OR REPLACE INTO macros (host, key, value, nextUpdate) VALUES (?, ?, ?, (strftime('%s', 'now')+ ? + random() % ?))''', (host, k, v, settings['macroRefreshInterval'], settings['macroRefreshInterval'] / 10))
                logging.debug("Caching macro [%s]: %s = %s" % (host, k, v))
                stats['macrosWrittenToCache'] += 1
            except sqlite3.IntegrityError as e:
                logging.warn("Duplicate Insert: %s" % e)
    mapDb.commit();

def refreshAllMacros(mapDb):
    conn = pymysql.connect(**settings['mysql'])

    startTime = time.time()
    macros = {}
    macrosRefreshed = 0;
    cur = conn.cursor()
    cur.execute('SELECT tt.host, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid')
    # Translation of item key_
    # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
    for row in cur:
        host = row[0];
        if not host:
            host = '__global__'
        if host not in macros:
            macros[host] = {}
        macros[host][row[1]] = row[2]
        macrosRefreshed += 1

    for (host, hostMacros) in macros.items():
        print "tcollector.zabbix_bridge.macros.refresh.count %d %f collector=zabbix action=refresh macro_host=%s\n" % (time.time(), len(hostMacros), host)
    cur.close()
    conn.close()

    if len(macros) > 0:
        writeMacros(mapDb, macros)

    print "tcollector.zabbix_bridge.macros.refresh.count %d %f collector=zabbix action=refresh\n" % (time.time(), macrosRefreshed)
    print "tcollector.zabbix_bridge.macros.refresh.duration %d %f collector=zabbix action=refresh\n" % (time.time(), time.time() - startTime)
    print "tcollector.zabbix_bridge.macros.refresh.rate %d %f collector=zabbix action=refresh\n" % (time.time(), macrosRefreshed / (time.time() - startTime))
    hostCount = 0
    for row in mapDb.cursor().execute('SELECT distinct(host), count(host) FROM macros GROUP BY host'):
        print "tcollector.zabbix_bridge.macros.refresh.hosts %d %f collector=zabbix macro_host=%s\n" % (time.time(), row[1], row[0])
        hostCount += 1
    print "tcollector.zabbix_bridge.macros.hosts.total %d %f collector=zabbix\n" % (time.time(), hostCount)

    logging.info("Finished refreshing %d macros for %d hosts in %d seconds..." % (macrosRefreshed, hostCount, (time.time() - startTime)))
    return macrosRefreshed

def refreshHostMacros(mapDb, settings, host):
    if not host:
        host = '__global__'

    logging.debug("Fetching macros for host %s from zabbix" % (host))

    macros = {}
    startTime = time.time()
    conn = pymysql.connect(**settings['mysql'])
    cur = conn.cursor()

    if not host or host == u'__global__':
        cur.execute("SELECT tt.host, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid WHERE tt.host is null")
    else:
        cur.execute("SELECT tt.host, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid WHERE tt.host = %(host)s", {'host': host} )
    for row in cur:
        macros[row[1]] = row[2]
    cur.close()
    conn.close()

    if len(macros) > 0:
        writeMacros(mapDb, {host: macros});
        print "tcollector.zabbix_bridge.macros.refresh.host_count %d %f collector=zabbix action=refresh macro_host=%s\n" % (time.time(), len(macros), host)
        print "tcollector.zabbix_bridge.macros.refresh.host_duration %d %f collector=zabbix action=refresh macro_host=%s\n" % (time.time(), time.time() - startTime, host)
        print "tcollector.zabbix_bridge.macros.refresh.host_rate %d %f collector=zabbix action=refresh macro_host=%s\n" % (time.time(), len(macros) / (time.time() - startTime), host)
        logging.debug("Cached %d macros for [%s] in %dms." % (len(macros), host, (time.time() - startTime) * 1000))

    return len(macros)

def findExpiredMacros(mapDb):
    c = mapDb.cursor()
    macros = []
    # we refresh all macros for a given host, since a host may have new keys added
    for row in c.execute('''SELECT distinct(host),count(host) FROM macros where nextUpdate < strftime('%s', 'now') group by host'''):
        if row[0]:
            macros.append(row[0])
            stats['macrosExpired'] += row[1]
        else:
            macros.append('__global__')
    # if (len(macros) > 0):
    #     logging.info("%d hosts have expired macros that need to be refreshed: %s" % (len(macros), macros));
    return macros

def fetchMacros(mapDb, host):
    if not host:
        host = '__global__'
    logging.debug("Fetching macros for host %s from cache" % (host))
    macros = {}
    try:
        c = mapDb.cursor()
        numMacros = c.execute('''SELECT count(nextUpdate) as total, sum(case when nextUpdate <= strftime('%s', 'now') then 1 else 0 end) as expired from macros where host = ?''', (host,)).fetchone()
        # logging.info("fetchMacros: %s => %s" % (host, numMacros))
        if (numMacros[0] == 0) or (numMacros[1] > 0):
            refreshHostMacros(mapDb, settings, host);

        for row in c.execute('SELECT * FROM macros where host = ?', (host,)):
            macros[row[1]] = row[2]
            stats['macrosReadFromCache'] += 1
    except sqlite3.ProgrammingError as e:
        logging.error("%s: %s" % (e, host))
        exit(1)

    return macros

def storeItems(mapDb, items):
    c = mapDb.cursor()
    for (itemId, item) in items.items():
        logging.debug("Caching item [%8s]: %s" % (itemId, item))
        try:
            c.execute('''INSERT OR REPLACE INTO items (itemId, host, key, metric, nextUpdate) VALUES (?, ?, ?, ?, (strftime('%s', 'now') + ? + random() % ?))''',
                      (itemId, item['_host'], item['_key'], item['metric'], settings['itemRefreshInterval'], settings['itemRefreshInterval'] / 10))
            # c.execute('DELETE FROM tags WHERE itemId=?', (itemId));
            for (tagk, tagv) in item['tags'].items():
                c.execute('''INSERT OR REPLACE INTO tags (itemId, tagk, tagv) VALUES (?, ?, ?)''', (itemId, tagk, tagv));
            stats['itemsWrittenToCache'] += 1
        except KeyError:
            # TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
            logging.error("Unable to write item to cache: %s => %s" % (itemId, item))
    mapDb.commit()

def getItemCacheStatus(mapDb):
    c = mapDb.cursor()
    stats = {}
    for row in c.execute('''select sum(case when nextUpdate <= strftime('%s', 'now') then 1 else 0 end) as expired, sum(case when nextUpdate > strftime('%s', 'now') then 1 else 0 end) as active, count(nextUpdate) as total from items'''):
        stats['expired'] = row[0]
        stats['active'] = row[1]
        stats['total'] = row[2]
        stats['apogee'] = float(row[1]) / float(row[2])

    stats['itemsPerHost'] = {}
    for row in c.execute('''select host, count(*) from items group by host'''):
        stats['itemsPerHost'][row[0]] = row[1]

    return stats

def mapItemKey(item, item_key):
    dct = parseZabbixKey(item_key, settings['mappings']['item-key'])
    if dct:
        metric = dct['metric']
        tags = dct['tags']

        logging.info("Parsed item-key [ %s ]: %s => %s" % (item_key, metric, tags))
        item['metric'] = metric;
        item['tags'].update(tags);
        return item;

    return None

def mapItemHost(item, item_host):
    # do item-host matchings -- the results of item-key and item-hosts are combined... item-hosts are not allowed to specify a metric name
    for (key, val) in settings['mappings']['item-host'].items():
        try:
            match = re.match(key, item_host)
            if match:
                tags = {}
                # match substitutions for the tag
                for (tk, tv) in val.items():
                    tk = match.expandf(tk)
                    tv = match.expandf(tv)
                    if tk not in tags:
                        tags[tk] = tv
                logging.debug("MATCHED item-host [ %s ] %s => %s" % (item_host, key, tags))
                item['tags'].update(tags);
                return item
        except re.error as error:
            logging.warn("Error matching regex [ %s ]: %s" % (key, error))
        except Exception as error:
            logging.error("matchItemHost[ %s ]: %s" % (item_host, error))
    return None

def refreshItem(mapDb, settings, itemId):
    startTime = time.time()
    conn = pymysql.connect(**settings['mysql'])

    item_key = None
    # cur.execute("SELECT tt.host as HOST_NAME, ht.hostid as HOST_ID, h.host as TEMPLATE_NAME, m.hostid as TEMPLATE_ID, h2.host as PROXY, m.hostmacroid, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid;
    cur = conn.cursor()
    logging.debug("Fetching details from Zabbix for item %s" % itemId)
    cur.execute('SELECT i.itemid, h.host, i.key_, h2.host AS proxy FROM items i JOIN hosts h ON i.hostid=h.hostid LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid where h.status<>3 and i.itemid = %(item)s', {'item': itemId})
    row = cur.fetchone()
    if row:
        logging.debug("Found Zabbix item %s :: %s @ %s" % (itemId, row[2], row[1]))
        item_host = row[1]
        macroMap = fetchMacros(mapDb, item_host);
        item_key = doMacroExpansion(macroMap, item_host, row[2])
        if item_key is None:
            logging.warn("Unmatched macro found in Zabbix item for host '%s' : %s" % (item_host, item_key))
            return { '_host': row[1], '_key': row[2] }
    else:
        logging.error("Unable to locate zabbix item (itemId): %s" % (itemId))
        return None

    # metric is the newly (and regularly) transformed zabbix item... we still us item_key throughout the code below.
    refreshedItem = parseZabbixKey( item_key, settings['mappings']['item-key'] )
    if refreshedItem:
        refreshedItem['_key'] = item_key
        refreshedItem['_host'] = item_host
        mapItemHost(refreshedItem, item_host)
        if 'tags' in refreshedItem:
            if 'host' not in refreshedItem['tags']:
                refreshedItem['tags']['host'] = item_host # re.sub(disallow, '-', item_host)

    #refreshedItem['tags']['original_host'] = re.sub(disallow, '_', item_host)
    # tags['original_item_key'] = re.sub(disallow, '_', item_key)

    #if row[3]:
    #    refreshedItem['tags']['proxy'] = row[3]
    #refreshedItem['tags']['collector'] = 'zabbix'

    cur.close()
    conn.close()

    if refreshedItem:
        storeItems(mapDb, {itemId: refreshedItem})
        buildTime = time.time() - startTime;
        logging.debug("refreshItem: Built item %s in %s seconds" % (itemId, buildTime))
        stats['buildTime'] += buildTime
        stats['buildCount'] += 1

    return refreshedItem;

def fetchItem(mapDb, itemId):
    startTime = time.time();
    try:
        c = mapDb.cursor()
        item = {}
        c.execute('''SELECT * FROM items where itemId = ?''', (itemId,))
        row = c.fetchone()
        if (row is not None) and (row[4] > time.time()):
            item['_host'] = row[1]
            item['_key'] = row[2]
            item['metric'] = row[3]
            stats['itemsReadFromCache'] += 1
        elif (row is not None) and (row[4] <= time.time()):
	    item = refreshItem(mapDb, settings, itemId)
            #logging.info("fetchItem: Requested item [%s] was expired. Refreshing..." % itemId)
        else:
            return None

        if item:
            logging.debug("fetchItem: Loading tags for %s from cache" % (itemId))
            tags = {}
            for row in c.execute('''SELECT * FROM tags where itemId = ?''', (itemId,)):
                tags.update({row[1]: row[2]})
            item['tags'] = tags
            logging.debug("fetchItem: Loaded tags for %s :: %s" % (itemId, tags))
    except ValueError as e:
        logging.error("%s on item %s" % (e, itemId))

    fetchTime = time.time() - startTime;
    logging.debug("fetchItem: Found %s in cache :: %s (%f secs)" % (itemId, item, fetchTime))
    stats['fetchTime'] += fetchTime
    stats['fetchCount'] += 1
    return item

def dumpDict(filename, d):
    f = open(filename, "w+")
    json.dump(d, f, indent=4, sort_keys=True)
    f.close()

def fetchDict(filename):
    try:
        f = open(filename, "r")
        d = json.load(f)
        f.close()
    except:
        d = None
    return d

def camelCase(s):
    s = re.sub(disallow, '', s)
    return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)

def doZabbixStream(mapDb):
    # Set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=settings['mysql'],
                                server_id=settings['slaveid'],
                                only_events=[WriteRowsEvent],
                                resume_stream=True,
                                blocking=True)

    lastLogTime = lastAlertTime = startTime = time.time()
    bestTimeDelta = timeDelta = 0
    lastItemTime = 0
    invalidItemId = 0
    lastBuildCount = 0
    lastFetchCount = 0
    lastSkippedCount = 0

    for binlogevent in stream:
        if binlogevent.schema == settings['mysql']['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    stats['rowCount'] += 1
                    stats['received'] += 1

                    r = row['values']
                    itemid = r['itemid']

                    lastDataTime = time.time()
                    if r['clock'] > lastItemTime:
                        lastItemTime = r['clock']

                    # drop data points that are more then 15 minutes old (this should keep us current, or at least 15 minutes current)
                    if (time.time() - r['clock']) > 300:
                        stats['rowsSkipped'] += 1
                        continue

                    hostItem = None;
                    try:
                        hostItem = fetchItem(mapDb, itemid);
                        if not hostItem:
                            hostItem = refreshItem(mapDb, settings, itemid)
                            if hostItem:
                                stats['updated'] += 1
                                logging.info("Added itemId %s [%s] to item cache." % (itemid, hostItem['_key']))
                            else:
                                logging.info("Received Zabbix Item ID that couldn't be correlated back to an item in Zabbix: %s" % (itemid))

                        if (hostItem is not None) and ('metric' in hostItem):
                            metric = sanitizeMetric(hostItem['metric'])
                            tags = sanitizeTags(hostItem['tags'])

                            print "%s %d %s" % (metric, r['clock'], r['value']),
                            for (tagk, tagv) in tags.items():
                                if (tagv is not None) and (tagv != ''):
                                    print "%s=%s" % (tagk, tagv),
                            print ""
                            stats['sent'] += 1
                        else:
                            stats['errors'] += 1
                            continue

                    except TypeError as e:
                        logging.error("FATAL: doZabbixStream: %s" % (e))
                        logging.error( traceback.format_exc() )
                        stats['errors'] += 1
                        exit(1)

                    # except KeyError as e:
                    #     TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
                    #     logging.error("ERROR: %s: %s" % (e, itemid))
                    #     stats['errors'] += 1

                    sys.stdout.flush()

                    # do some metric calculations (and/or logging alerts)
                    timeDelta = lastDataTime - lastItemTime
                    if (time.time() - lastAlertTime) > 120:
                        if timeDelta > 300:
                            logging.info("Processing %d zabbix rows/sec." % (stats['rowCount'] / (time.time() - lastLogTime)))
                            logging.warn("Zabbix-collector is %d seconds behind the master db." % timeDelta)
                            lastAlertTime = time.time();

                if (time.time() - lastDataTime) > 30:
                    if nextAlertTime < time.time():
                        logging.warn("Last zabbix data was %d seconds ago...  is Zabbix running???" % (time.time() - lastDataTime))
                        nextAlertTime = time.time() + 30

        # write zabbix_collector metric to opentsdb
        lastLogInterval = time.time() - lastLogTime
        if lastLogInterval > 15:
            itemsPerSec = stats['rowCount'] / lastLogInterval
            itemCacheStats = getItemCacheStatus(mapDb);
            logging.info("Zabbix Stats: %d total cached items, %d (%d%%) active, %d expired. %d rows processed in %s seconds.  %d items/sec. %d seconds behind." % (itemCacheStats['total'], itemCacheStats['active'], itemCacheStats['apogee'] * 100, itemCacheStats['expired'], stats['rowCount'], lastLogInterval, itemsPerSec, timeDelta));
            # print "tcollector.items.keyErrors %d %f collector=zabbix host=%s\n" % (time.time(), invalidItemId, socket.getfqdn())
            # print "tcollector.zabbix_bridge.items.cache.expired %d %f collector=zabbix\n" % (time.time(), stats['itemsExpired'])
            print "tcollector.zabbix_bridge.items.cache.apogee %d %f collector=zabbix" % (time.time(), itemCacheStats['apogee'])
            for (host,n) in itemCacheStats['itemsPerHost'].items():
                print "tcollector.zabbix_bridge.items.cache.count.per_host %d %f collector=zabbix item_host=%s\n" % (time.time(), n, sanitizeMetric(host))
            print "tcollector.zabbix_bridge.items.cache.count.expired %d %f collector=zabbix" % (time.time(), itemCacheStats['expired'])
            print "tcollector.zabbix_bridge.items.cache.count.total %d %f collector=zabbix" % (time.time(), itemCacheStats['total'])
            print "tcollector.zabbix_bridge.items.cache.count.reads %d %f collector=zabbix\n" % (time.time(), stats['itemsReadFromCache'])
            print "tcollector.zabbix_bridge.items.cache.count.writes %d %f collector=zabbix\n" % (time.time(), stats['itemsWrittenToCache'])
            if stats['buildCount'] > 0:
                buildCount = stats['buildCount'] - lastBuildCount
                buildTime = stats['buildTime'] / stats['buildCount']
                buildRate = (buildCount * buildTime) / lastLogInterval
                print "tcollector.zabbix_bridge.items.cache.time.writes %d %f collector=zabbix\n" % (time.time(), buildTime)
                print "tcollector.zabbix_bridge.items.cache.throughput.writes %d %f collector=zabbix\n" % (time.time(), buildRate)
                lastBuildCount = stats['buildCount']
            if stats['fetchCount'] > 0:
                fetchCount = stats['fetchCount'] - lastFetchCount
                fetchTime = stats['fetchTime'] / stats['fetchCount']
                fetchRate = (fetchCount * fetchTime) / lastLogInterval
                print "tcollector.zabbix_bridge.items.cache.time.reads %d %f collector=zabbix\n" % (time.time(), fetchTime)
                print "tcollector.zabbix_bridge.items.cache.throughput.reads %d %f collector=zabbix\n" % (time.time(), fetchRate)
                lastFetchCount = stats['fetchCount']

            for row in mapDb.cursor().execute('SELECT count(distinct(host)) FROM macros'):
                print "tcollector.zabbix_bridge.macros.refresh.hosts %d %f collector=zabbix\n" % (time.time(), row[0])
            for row in mapDb.cursor().execute('SELECT distinct(host), count(host) FROM macros GROUP BY host'):
                print "tcollector.zabbix_bridge.macros.count.per_host %d %f collector=zabbix macro_host=%s\n" % (time.time(), row[1], row[0])
            print "tcollector.zabbix_bridge.macros.expired %d %f collector=zabbix\n" % (time.time(), stats['macrosExpired'])
            print "tcollector.zabbix_bridge.macros.writtenToCache %d %f collector=zabbix\n" % (time.time(), stats['macrosWrittenToCache'])
            print "tcollector.zabbix_bridge.macros.readFromCache %d %f collector=zabbix\n" % (time.time(), stats['macrosReadFromCache'])

            print "tcollector.items.errors %d %f collector=zabbix\n" % (time.time(), stats['errors'])
            print "tcollector.items.sent %d %f collector=zabbix\n" % (time.time(), stats['sent'])
            print "tcollector.items.received %d %f collector=zabbix\n" % (time.time(), stats['received'])
            print "tcollector.items.skipped %d %f collector=zabbix\n" % (time.time(), stats['rowsSkipped'])
            print "tcollector.items.updated %d %f collector=zabbix\n" % (time.time(), stats['updated'])
            print "tcollector.items.errors %d %f collector=zabbix\n" % (time.time(), stats['errors'])
            print "tcollector.items.sent %d %f collector=zabbix\n" % (time.time(), stats['sent'])
            print "tcollector.items.perSecond %d %f collector=zabbix\n" % (time.time(), itemsPerSec)
            print "tcollector.collector.delay %d %f collector=zabbix\n" % (time.time(), timeDelta)
            stats['rowCount'] = 0
            lastLogTime = time.time()

    stream.close()

def doMacroExpansion(macroMap, item_host, item_key):
    match = re.search("\{\$\w+\}", item_key)
    if match:
        logging.debug("Doing Macro Expansion: %s : %s" % (item_host, item_key))
        while match:
            # process Zabbix macros first -- this ONLY does the incoming item key (the idea being that all tag values later are pulled from the item-key)
            if match[0] in macroMap:
                item_key = re.sub(re.escape(match[0]), macroMap[match[0]], item_key);
                logging.debug("MACRO EXPAND [%s]: %s" % (item_host, item_key));
            elif match[0] in settings['mappings']['macros']:
                item_key = re.sub(re.escape(match[0]), settings['mappings']['macros'][match[0]], item_key);
                logging.debug("GLOBAL MACRO EXPAND: %s [%s]" % (item_key, match[0]));
            else:
                break;
            match = re.search("\{\$\w+\}", item_key)

        match = re.search("\{\$\w+\}", item_key)
        if match:
            logging.warn("Unmatched macro found in Zabbix item for host '%s' : %s" % (item_host, match[0]))
            item_key = None

    return item_key

def isNumber(n):
    try:
        return float(n)
    except ValueError:
        return False

def expandParameters(stringIn, paramMap, context=''):
    # first check to see if there are any macros
    match = re.search("\{@([\w:\.]+)\}", stringIn)
    if match:
        logging.debug("Doing parameter expansion: %s" % (stringIn))
        while match:
            if match[1] in paramMap:
                stringIn = re.sub(re.escape(match[0]), paramMap[match[1]], stringIn);
                logging.debug("PARAMETER EXPAND: %s [%s]" % (stringIn, match[1]));
            else:
                break;
            match = re.search("\{@([\w:\.]+)\}", stringIn)

        match = re.search("\{@([\w:\.]+)\}", stringIn)
        if match:
            logging.warn(" [%s] : Unmatched parameter found in string : %s [ %s ] %s ... Ignoring." % (context, stringIn, match[0], paramMap))

    return stringIn

def argParser_default(match, config):
    logging.debug("defaultParser: [ match: %s ]" % (match.groups(),))

    key = {}
    tags = {}
    # match substitutions for the tags
    if 'tags' in config:
        for (k, v) in config['tags'].items():
            k = match.expandf(k)
            v = match.expandf(v)
            if k and v:
                tags[k] = v
    key['tags'] = tags
    key['metric'] = match.expandf(config['metric'])

    logging.debug("argParser_default: %s" % key)
    return key

def argParser_index(paramStr, config, match):
  #  'argParser': 'index', # which parser to use for this item's parameters
  #  'flags': { # parameters to pass to the argParder
  #      'csv-dialect': {
  #          'delimiter': ',', # the a delimiter pattern for parsing the args into columns
  #          'quotechar': ''', # the a delimiter pattern for parsing the args into columns
  #          'doublequote': True,
  #          'escapechar': '//',
  #          'skipinitialspace': False,
  #          'strict': False,
  #      },
  #      'parameterPrefix': 'arg', # a string prepended to names. If namedParameters is not specified, this is prefixed on the index: ie, 'arg' = ['arg1', 'arg2', 'arg3', ...]
  #      'namedParameters': ['type','device','path'] # an array of names that are matched to the index of the parameter, if not specified parameters are referenced by index {1}, which can be confusing between the top-level parser and this one
  #      'expandParameters': True # will add all parsed parameters to the tag list (otherwise, tags must be explicitely specified)
  #  }

    logging.debug("indexParser: [ paramStr: %s ]  [ match: %s ]" % (paramStr, (match.groups(),)))
    key = False

    flags = {}
    paramPrefix = ''
    dialect_overrides = {}
    paramsToTags = False
    parameterNames = None
    if 'flags' in config:
        flags = config['flags']
        if 'parameterPrefix' in flags:
            paramPrefix = flags['parameterPrefix']
        if 'dialect' in flags:
            dialect_overrides = flags['dialect']
        if 'expandParameters' in flags:
            paramsToTags = flags['expandParameters']
        if 'namedParameters' in flags:
            parameterNames = flags['namedParameters']

    args = {}
    for row in csv.reader([paramStr], dialect='zabbix1', **dialect_overrides):
        try:
            for (idx, param) in enumerate(row):
                if parameterNames is not None:
                    logging.debug("indexParser: {%s%s} => %s" % (paramPrefix, parameterNames[idx], param))
                    args['%s%s' % (paramPrefix, parameterNames[idx])] = param
                elif paramsToTags:
                    logging.debug("indexParser: {%s%s} => %s" % (paramPrefix, idx+1, param))
                    args['%s%s' % (paramPrefix, idx+1)] = param
                else:
                    logging.debug("indexParser: {%s} => %s" % (idx+1, param))
                    args[idx+1] = param

            key = {}
            tags = {}
            if paramsToTags:
                tags = dict(**args)

            if 'tags' in config:
                for (k, v) in config['tags'].items():
                    k = match.expandf(expandParameters(k, args, 'index: '+paramStr))
                    v = match.expandf(expandParameters(v, args, 'index: '+paramStr))
                    if k and v:
                        tags[k] = v
            key['tags'] = tags
            key['metric'] = match.expandf(expandParameters(config['metric'], args, 'index: '+paramStr))

        except Exception as e:
            logging.error("argParser_index: %s :: %s" % (e, row))

    logging.debug("argParser_index: %s" % key)
    return key

def argParser_named(paramStr, config, match):
  #  'argParser': 'named', # which parser to use for this item's parameters
  #  'flags': { # parameters to pass to the argParder
  #      'csv-dialect': {
  #          'delimiter': ',', # the a delimiter pattern for parsing the args into columns
  #          'quotechar': ''', # the a delimiter pattern for parsing the args into columns
  #          'doublequote': True,
  #          'escapechar': '//',
  #          'skipinitialspace': False,
  #          'strict': False,
  #      },
  #      'keyValyeSplitExpression': '=', # a regex expression passed to regex.split() that is used to split the named pairs into key and value; defaults to '='
  #      'parameterPrefix': '', # a string prepended to all names, ie, 'tag.' = 'tag.foo', 'tag.bar', etc
  #      'expandParameters': True # will add all parsed parameters to the tag list (otherwise, tags must be explicitely specified)
  #  }

    logging.debug("namedParser: [ paramStr: %s ]  [ match: %s ]" % (paramStr, (match.groups(),)))
    key = False

    flags = {}
    paramPrefix = ''
    dialect_overrides = {}
    paramsToTags = False
    keyValueSplitExpression = '='
    if 'flags' in config:
        flags = config['flags']
        if 'dialect' in flags:
            dialect_overrides = flags['dialect']
        if 'parameterPrefix' in flags:
            paramPrefix = flags['parameterPrefix']
        if 'expandParameters' in flags:
            paramsToTags = flags['expandParameters']
        if 'keyValueSplitExpression' in flags:
            keyValueSplitExpression = flags['keyValueSplitExpression']

    args = {}
    for row in csv.reader([paramStr], dialect='zabbix1', **dialect_overrides):
        try:
            for col in row:
                pair = col.split(keyValueSplitExpression)
                args['%s%s' % (paramPrefix, pair[0])] = pair[1]

            key = {}
            tags = {}
            key['metric'] = match.expandf(expandParameters(config['metric'], args, 'named: '+paramStr))
            if paramsToTags:
                tags = dict(**args)

            if 'tags' in config:
                for (k, v) in config['tags'].items():
                    k = match.expandf(expandParameters(k, args, 'named: '+paramStr))
                    v = match.expandf(expandParameters(v, args, 'named: '+paramStr))
                    if k and v:
                        tags[k] = v
            key['tags'] = tags

            if 'host' in key['tags']:
                host = key['tags']['host']
                del key['tags']['host']
                key['tags']['tagged_host'] = host

        except Exception as e:
            logging.error("argParser_named: %s :: %s" % (e, row))
            logging.error( traceback.format_exc() )

    logging.debug("argParser_named: %s" % key)
    return key

def argParser_jmx(paramStr, config, match):
  #  'argParser': 'jmx', # which parser to use for this item's parameters
  #  'flags': { # parameters to pass to the argParder
  #      'splitParameters': if True, attempt to split each the jmx args into key-value parameters?  if True, when use the splitExpression below
  #      'splitExpression': ',', # a regex expression passed to regex.split() that is used to split the parameters; defaults to ','
  #      'paramPrefix': 'jmx', # the name used to store the parameters, ie, 'jmx'.  Parameters are then expanded using {jmx[index]} or {jmx.name}
  #      'namedParameters': True
  #  }

    logging.debug("jmxParser: [ paramStr: %s ]  [ match: %s ]" % (paramStr, (match.groups(),)))
    key = False

    flags = {}
    paramPrefix = ''
    paramsToTags = False
    if 'flags' in config:
        flags = config['flags']
        if 'parameterPrefix' in flags:
            paramPrefix = flags['parameterPrefix']
        if 'expandParameters' in flags:
            paramsToTags = flags['expandParameters']

    for row in csv.reader([paramStr], 'zabbix1'):
        try:
            # print row
            # row = list(i.replace('"', '') for i in row)
            if ':' in row[0]:
                d = row[0].split(':')
            elif ',' in row[0]:
                d = row[0].split(',')
            else:
                break
            args = {}
            for i in d[1].split(','):
                pair = i.split('=')
                args['%s%s' % (paramPrefix, pair[0])] = pair[1]
            args['%sdomain' % paramPrefix] = d[0]
            args['%sattribute' % paramPrefix] = row[1]

            key = {}
            tags = {}

            key['metric'] = match.expandf(expandParameters(config['metric'], args))
            if paramsToTags:
                tags = dict(**args)
                del tags['%sdomain' % paramPrefix]
                del tags['%sattribute' % paramPrefix]

            if 'tags' in config:
                for (k, v) in config['tags'].items():
                    k = match.expandf(expandParameters(k, args))
                    v = match.expandf(expandParameters(v, args))
                    if k and v:
                        tags[k] = v
            key['tags'] = tags

            if 'host' in key['tags']:
                host = key['tags']['host']
                del key['tags']['host']
                key['tags']['jmx_host'] = host

        except Exception as e:
            logging.error("argParser_jmx: %s :: %s" % (e, row))
            logging.error( traceback.format_exc() )

    logging.debug("argParser_jmx: %s" % key)
    return key

def sanitizeTags(tags):
    try:
        for (tk, tv) in tags.items():
            del tags[tk]

            tk = transTagKeysRegex.sub(lambda mo: transTagKeys[mo.string[mo.start():mo.end()]], tk)
            tv = transTagValsRegex.sub(lambda mo: transTagVals[mo.string[mo.start():mo.end()]], tk)

#            tk = re.sub('^\"(.*)\"$', '\\1', tk)  # strip leading/trailing quotes
#            tk = re.sub('\s([A-Z])', '\\1', tk)    # strip spaces followed by a capital letter (Camel Case => CamelCase)
#            tk = re.sub('\s([a-z])', '_\\1', tk)   # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
#            tk = re.sub('\s([0-9])', '.\\1', tk)   # replace spaces followed by a number with a dot (item 1 => item.1)
#            tk = re.sub('^([A-Z])', lambda p: p.group(1).lower(), tk) # lower-case the first letter of the tag key, if its not
#            tk = re.sub(disallow, '', tk)

#            tv = re.sub('^\"(.*)\"$', '\\1', tv)  # strip leading/trailing quotes
#            tv = re.sub('\s([A-Z])', '\\1', tv)    # strip spaces followed by a capital letter (Camel Case => CamelCase)
#            tv = re.sub('\s([a-z])', '_\\1', tv)   # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
#            tv = re.sub('\s([0-9])', '.\\1', tv)   # replace spaces followed by a number with a dot (item 1 => item.1)
#            tv = re.sub(disallow, '', tv)

            tags[tk] = tv
        return tags
    except Exception as e:
        logging.error('ERROR Sanitizing Tags: %s : %s' % (e, tags))

def sanitizeMetric(metric):
    metricIn = metric
    try:
        metric = transMetricRegex.sub(lambda mo: transTagVals[mo.string[mo.start():mo.end()]], metric)
#        metric = re.sub('^\"(.*)\"$', '\\1', metric)  # strip leading/trailing quotes
#        metric = re.sub('\s([A-Z])', '\\1', metric)    # strip spaces followed by a capital letter (Camel Case => CamelCase)
#        metric = re.sub('\s([a-z])', '_\\1', metric)   # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
#        metric = re.sub('\s([0-9])', '.\\1', metric)   # replace spaces followed by a number with a dot (item 1 => item.1)
#        #metric = re.sub('^([A-Z])', lambda p: p.group(1).lower(), metric) # lower-case the first letter of the metric, if its not
#        metric = re.sub(disallow, '', metric) # final scrub for invalid characters (strip them)
        if metric != metricIn:
            logging.debug("sanitizeMetric [ %s ] => %s" % (metricIn, metric))
        return metric
    except Exception as e:
        logging.error('ERROR Sanitizing Metric: %s : %s' % (e, metric))

availableParsers = dict((key, value) for key,value in globals().iteritems() if key.startswith('argParser'))

def parseZabbixKey(item_key, itemMappings):
    # all zabbix _keys look like this 'name[arg1,arg2,arg3,...,argn]'
    logging.debug("parseZabbixKey: %s" % item_key)
    key = {}
    for (pattern, config) in itemMappings.items():
        m = re.match(pattern, item_key)
        if m:
            argParser = None
            argString = m.group(0)
            if 'argParser' in config:
                argParser = 'argParser_%s' % config['argParser']
                logging.debug("parseZabbixKey: argParser = %s" % argParser)
            if 'argString' in config:
                argString = m.expandf(config['argString'])
                logging.debug("parseZabbixKey: argString = %s" % argString)

            if argParser:
                if argParser in availableParsers:
                    key = availableParsers[argParser](argString, config, m)
                else:
                    logging.error("Invalid parser: [ %s ] => %s" % (argParser, availableParsers))
            else:
                key = argParser_default(m, config)

            logging.debug("parseZabbixKey:  '%s'  matched '%s' => %s" % (item_key, pattern, key))
            return key

    logging.error('Unable to match key from zabbix: %s' % (item_key))
    return False

def main():
    utils.drop_privileges()
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1

    logging.info("Starting zabbix-bridge....")
    mapDb = sqlite3.connect("/tmp/zabbixMap.sqlite")
    # mapDb = sqlite3.connect(":memory:")
    mapDb.isolation_level = 'EXCLUSIVE'
    configCache(mapDb)
    # refreshAllMacros(mapDb, settings);
    while True:
        cProfile.runctx('doZabbixStream(mapDb)', globals(), {'mapDb': mapDb}, '/tmp/zabbix_bridge.profile')
        logging.info("Waiting for stuff to do...")

    mapDb.close()

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())


## Sample zabbix debug dump:
# === WriteRowsEvent ===
# Date: 2014-08-04T03:47:37
# Log position: 249670
# Event size: 135
# Read bytes: 10
# Table: zabbix.history
# Affected columns: 4
# Changed rows: 5
# Values:
# --
# ('*', u'itemid', ':', 23253)
# ('*', u'ns', ':', 14761117)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124053)
# --
# ('*', u'itemid', ':', 23254)
# ('*', u'ns', ':', 19470979)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124054)
# --
# ('*', u'itemid', ':', 23255)
# ('*', u'ns', ':', 19872263)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124055)
# --
# ('*', u'itemid', ':', 23256)
# ('*', u'ns', ':', 20960622)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124056)
# --
# ('*', u'itemid', ':', 23257)
# ('*', u'ns', ':', 22024251)
# ('*', u'value', ':', 0.0254)
# ('*', u'clock', ':', 1407124057)
# ()
