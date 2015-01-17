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
from collections import OrderedDict
import simplejson as json
import time
import logging
import socket

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
logging.basicConfig(filename=logFileName, level=logLevel)

stats = {}
stats['received'] = 0
stats['errors'] = 0
stats['sent'] = 0

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


def parseZabbixKey(item_key):
    # all zabbix _keys look like this 'name[arg1,arg2,arg3,...,argn]'
    key = {}
    m = re.match('([^\[]*)(\[([^\]]*)\])?', item_key)
    if m:
        key['name'] = m.group(1)
        if m.lastindex <= 1:
            key['args'] = []
        else:
            args = m.group(3)
            for row in csv.reader([args], 'zabbix1'):
                row = list(i.replace('"', '') for i in row)
            key['args'] = row
        return key

    logging.error('Unable to match key from zabbix: %s' % (item_key))
    return False


def doZabbixStream(hostMap):
    # Set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=settings['mysql'],
                                server_id=settings['slaveid'],
                                only_events=[WriteRowsEvent],
                                resume_stream=True,
                                blocking=True)

    lastLogTime = lastAlertTime = startTime = time.time()
    rowCount = 0
    timeDelta = 0
    lastItemTime = 0
    invalidItemId = 0

    if len(hostMap) == 0:
        logging.error("Invalid hostMap contains no items.")
        return 1
    logging.info("hostMap contains %d items." % (len(hostMap)))

    for binlogevent in stream:
        if binlogevent.schema == settings['mysql']['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    rowCount += 1
                    stats['received'] += 1

                    r = row['values']
                    itemid = r['itemid']

                    lastDataTime = time.time()
                    if r['clock'] > lastItemTime:
                        lastItemTime = r['clock']

                    hostItem = None;
                    try:
                        hostItem = hostMap.get(u'%d' % (itemid))
                        if (hostItem is not None) and ('metric' in hostItem):
                            print "%s %d %s" % (hostItem['metric'], r['clock'], r['value']),
                            for (tagk, tagv) in hostItem['tags'].items():
                                if (tagv is not None) and (tagv != ''):
                                    print "%s=%s" % (camelCase(tagk), re.sub(disallow, '', tagv)),
                            print ""
                            stats['sent'] += 1
                        else:
                            if (hostItem is None):
                                logging.warn("Itemid not found in host map: %s" % (itemid))
                            else:
                                logging.warn("Referenced item with no metric (probably invalid macro expansion): %s" % (hostItem))
                            stats['errors'] += 1
                            continue

                    except TypeError as e:
                        logging.error("Type Error: %s" % (e))
                        stats['errors'] += 1

                    except KeyError:
                        # TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
                        logging.error("ERROR: Unable to find itemId in host map: %s" % (itemid))
                        stats['errors'] += 1

                    sys.stdout.flush()

                    # do some metric calculations (and/or logging alerts)
                    timeDelta = lastDataTime - lastItemTime
                    if (time.time() - lastAlertTime) > 120:
                        if timeDelta > 300:
                            logging.info("Processing %d zabbix rows/sec." % (rowCount / (time.time() - lastLogTime)))
                            logging.warn("Zabbix-collector is %d seconds behind the master db." % timeDelta)
                            lastAlertTime = time.time();

                if (time.time() - lastDataTime) > 30:
                    if nextAlertTime < time.time():
                        logging.warn("Last zabbix data was %d seconds ago...  is Zabbix running???" % (time.time() - lastDataTime))
                        nextAlertTime = time.time() + 30

        # write zabbix_collector metric to opentsdb
        if (time.time() - lastLogTime) > 5:
            itemsPerSec = rowCount / (time.time() - lastLogTime)
            # print "tcollector.items.keyErrors %d %f collector=zabbix host=%s\n" % (time.time(), invalidItemId, socket.getfqdn())
            print "tcollector.items.received %d %f collector=zabbix host=%s\n" % (time.time(), stats['received'], socket.getfqdn())
            print "tcollector.items.errors %d %f collector=zabbix host=%s\n" % (time.time(), stats['errors'], socket.getfqdn())
            print "tcollector.items.sent %d %f collector=zabbix host=%s\n" % (time.time(), stats['sent'], socket.getfqdn())
            print "tcollector.items.perSecond %d %f collector=zabbix host=%s\n" % (time.time(), itemsPerSec, socket.getfqdn())
            print "tcollector.collector.delay %d %f collector=zabbix host=%s\n" % (time.time(), timeDelta, socket.getfqdn())
            rowCount = 0
            lastLogTime = time.time()

    logging.info("No more zabbix events in the database... shutting down.")
    stream.close()


def doMacroExpansion(macroMap, item_host, item_key):
    if re.search("\{\$\w+\}", item_key) == None:
        return item_key # nothing to do

    logging.debug("Doing Macro Expansion: %s : %s" % (item_host, item_key))
    # process Zabbix macros first -- this ONLY does the incoming item key (the idea being that all tag values later are pulled from the item-key)
    if item_host in macroMap:
        hostMacros = macroMap[item_host]
        for (mk, mv) in hostMacros.items():
            item_key = re.sub(re.escape(mk), mv, item_key);

    # then look for additional local macro overrides (from settings)
    for (mk, mv) in settings['mappings']['macros'].items():
        match = re.match(mk, item_host)
        if match:
            for (mk, mv) in mv.items():
                item_key = re.sub(re.escape(mk), mv, item_key);

    match = re.search("\{\$\w+\}", item_key)
    if match:
        logging.warn("Unmatched macro found in zabbix host/item-key: %s == %s" % (item_host, item_key))
        item_key = None

    return item_key


def argParser_jmxParser(flags, paramStr):
#                'argParser': 'jmxParser', # which parser to use for this item's parameters
#                'flags': { # parameters to pass to the argParder
#                        'splitParameters': if True, attempt to split each the jmx args into key-value parameters?  if True, when use the splitExpression below
#                        'splitExpression': ',', # a regex expression passed to regex.split() that is used to split the parameters; defaults to ','
#                        'paramPrefix': 'jmx', # the name used to store the parameters, ie, 'jmx'.  Parameters are then expanded using {jmx[index]} or {jmx.name}
#                        'namedParameters': True
#                    }

    tags = False
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
            tags = dict(i.split('=') for i in d[1].split(','))
            tags['domain'] = d[0]
            tags['attribute'] = row[1]

            for (tk, tv) in tags.items():
                del tags[tk]
                tk = re.sub('^\"(.*)\"$', '\\1', tk)  # strip leading/trailing quotes
                tk = re.sub('\s([A-Z])', '\\1', tk)    # strip spaces followed by a capital letter (Camel Case => CamelCase)
                tk = re.sub('\s([a-z])', '_\\1', tk)   # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
                tk = re.sub('\s([0-9])', '.\\1', tk)   # replace spaces followed by a number with a dot (item 1 => item.1)
                tk = re.sub('^([A-Z])', lambda p: p.group(1).lower(), tk) # lower-case the first letter of the tag key, if its not
                tk = re.sub(disallow, '', tk)

                tv = re.sub('^\"(.*)\"$', '\\1', tv)  # strip leading/trailing quotes
                tv = re.sub('\s([A-Z])', '\\1', tv)    # strip spaces followed by a capital letter (Camel Case => CamelCase)
                tv = re.sub('\s([a-z])', '_\\1', tv)   # replace spaces followed by a lower-case letter with an underscore (Camel case => camel_case)
                tv = re.sub('\s([0-9])', '.\\1', tv)   # replace spaces followed by a number with a dot (item 1 => item.1)
                tv = re.sub(disallow, '', tv)

                tags[tk] = tv

            # print "%s.%s ==> %s " % (domain, attribute, tags)
        except IndexError as e:
            logging.err("Error parsing JMX parameters: %s" % e)
    return tags

def doJmxItems(item):
    tags = argParser_jmxParser({}, item)
    if 'host' in tags:
        host = tags['host']
        del tags['host']
        tags['jmx_host'] = host

    dct = {}
    dct['metric'] = "jmx.%s.%s" % (subtype, row[1])
    tags['collector'] = 'zabbix'
    dct['tags'] = tags

    return dct


def buildMacroMap(conn, settings):
    cur = conn.cursor()
    # cur.execute("SELECT tt.host as HOST_NAME, ht.hostid as HOST_ID, h.host as TEMPLATE_NAME, m.hostid as TEMPLATE_ID, h2.host as PROXY, m.hostmacroid, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid;
    cur.execute("SELECT hostmacroid from zabbix.hostmacro order by hostmacroid desc limit 1");
    lastMacroId = cur.fetchone()[0]
    cur.close()

    startTime = time.time()
    macroMap = fetchDict("/tmp/tcollector.zabbix_bridge.macro_map.log");
    print "tcollector.zabbix_bridge.macroMap.duration %d %f collector=zabbix action=load host=%s\n" % (time.time(), time.time() - startTime, socket.getfqdn())
    logging.info("Loaded MacroMap cache in %d seconds..." % (time.time() - startTime))

    # do we need to rebuild the macro map??  If not, then just load the previous dict from disk
    if (macroMap is not None) and ('lastMacroId' in macroMap) and (lastMacroId <= macroMap['lastMacroId']) and ('lastBuildTime' in macroMap) and ((time.time() - macroMap['lastBuildTime']) < settings['macroMapInterval']):
        logging.info("Using cached MacroMap...")
        return macroMap

    startTime = time.time()
    logging.info("Building MacroMap...")
    macroMap = {}
    macroMap['hosts'] = {}
    macroMap['lastMacroId'] = lastMacroId

    cur = conn.cursor()
    cur.execute("SELECT tt.host, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid")
    # Translation of item key_
    # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags

    for row in cur:
        if row[0] not in macroMap['hosts']:
            macros = {}
        else:
            macros = macroMap['hosts'][row[0]]

        macros[row[1]] = row[2]
        macroMap['hosts'][row[0]] = macros

    cur.close()

    macroMap['lastBuildTime'] = time.time()
    print "tcollector.zabbix_bridge.macroMap.time %d %f collector=zabbix action=build host=%s\n" % (time.time(), time.time(), socket.getfqdn())
    print "tcollector.zabbix_bridge.macroMap.duration %d %f collector=zabbix action=build host=%s\n" % (time.time(), time.time() - startTime, socket.getfqdn())
    if 'host' in macroMap:
        print "tcollector.zabbix_bridge.macroMap.host.count %d %f collector=zabbix host=%s\n" % (time.time(), len(macroMap['hosts']), socket.getfqdn())
    for (h, m) in macroMap['hosts'].items():
        if isinstance(m, dict):
            print "tcollector.zabbix_bridge.macroMap.count %d %f collector=zabbix host=%s macroHost=%s\n" % (time.time(), len(m.keys()), socket.getfqdn(), h)

    logging.info("Finished building MacroMap in %d seconds..." % (time.time() - startTime))
    logging.info("Writing MacroMap to disk...")
    dumpDict("/tmp/tcollector.zabbix_bridge.macro_map.log", macroMap);
    return macroMap['hosts']


def buildHostMap(settings):

    conn = pymysql.connect(**settings['mysql'])
    macroMap = buildMacroMap(conn, settings)

    cur = conn.cursor()
    cur.execute("SELECT itemid from zabbix.items order by itemid desc limit 1");
    lastItemId = cur.fetchone()[0]
    cur.close()

    startTime = time.time()
    hostMap = fetchDict("/tmp/tcollector.zabbix_bridge.host_map.log");
    print "tcollector.zabbix_bridge.hostMap.duration %d %f collector=zabbix action=load host=%s\n" % (time.time(), time.time() - startTime, socket.getfqdn())
    logging.info("Loaded HostMap cache in %d seconds..." % (time.time() - startTime))

    # do we need to rebuild the macro map??  If not, then just load the previous dict from disk
    if (hostMap is not None) and ('lastItemId' in hostMap) and (lastItemId <= hostMap['lastItemId']) and ('lastBuildTime' in hostMap) and ((time.time() - hostMap['lastBuildTime']) < settings['hostMapInterval']):
        logging.info("No changes between cache and zabbix HostMap.  Using cached HostMap...")
        return hostMap

    startTime = time.time()
    logging.info("Building HostMap...")
    hostMap = {}
    hostMap['lastItemId'] = lastItemId
    hostMap['items'] = {}


    cur = conn.cursor()
    # cur.execute("SELECT tt.host as HOST_NAME, ht.hostid as HOST_ID, h.host as TEMPLATE_NAME, m.hostid as TEMPLATE_ID, h2.host as PROXY, m.hostmacroid, m.macro, m.value FROM zabbix.hostmacro m JOIN hosts h on m.hostid=h.hostid LEFT JOIN hosts h2 on h2.hostid=h.proxy_hostid LEFT JOIN hosts_templates ht on ht.templateid=h.hostid LEFT JOIN hosts tt on tt.hostid=ht.hostid;
    cur.execute("SELECT i.itemid, h.host, i.key_, h2.host AS proxy FROM items i JOIN hosts h ON i.hostid=h.hostid LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid where h.status<>3")
    for row in cur:
        item_host = row[1]
        item_key = doMacroExpansion(macroMap, item_host, row[2])
        if item_key is None:
            hostMap['items'][row[0]] = {'_host': row[1], '_key': row[2] } # flag this; don't process this item/host id
            continue

        # metric is the newly (and regularly) transformed zabbix item... we still us item_key throughout the code below.
        logging.debug('%s' % parseZabbixKey(item_key))
        metric = item_key
        tags = {}

        # JMX has some funky matching rules that are beyond the simple capabilities of the config file
        match = re.match("jmx\[([^\]]+)\]", item_key)
        if match:
            dct = doJmxItems(match.group(1))
            metric = dct['metric']
            tags = dct['tags']
            # logging.debug('JMX Item: %s :: %s ==> %s\n' % (item_key, metric, tags))
        else:
            # do item-key matchings
            for (key, val) in settings['mappings']['item-key'].items():
                try:
                    match = re.match(key, item_key)
                    if match:
                        # match substitutions for the metric value
                        metric = match.expandf(val['metric'])
                        # match substitutions for the tags
                        if 'tags' in val:
                            for (tk, tv) in val['tags'].items():
                                tk = match.expandf(tk)
                                tv = match.expandf(tv)
                                tags[tk] = tv
                        logging.debug("MATCHED item-key [ %s ] using [ %s ]: %s => %s" % (item_key, key, metric, tags))
                        break
                except IndexError as error:
                    logging.warn("Error Index: %s on item_key %s" % (error, item_key))
                except re.error as error:
                    logging.warn("Error matching regex [ %s ]: %s on item_key %s" % (key, error, item_key))

        # do item-host matchings -- the results of item-key and item-hosts are combined... item-hosts are not allowed to specify a metric name
        for (key, val) in settings['mappings']['item-host'].items():
            try:
                match = re.match(key, item_host)
                if match:
                    # match substitutions for the tag
                    for (tk, tv) in val.items():
                        tk = match.expandf(tk)
                        tv = match.expandf(tv)
                        tags[tk] = tv
                    break
                    logging.debug("MATCHED item-host [ %s ] => %s" % (row[2], tags))
            except re.error as error:
                logging.warn("Error matching regex: %s" % (error))

        if 'host' not in tags:
            tags['host'] = item_host # re.sub(disallow, '-', item_host)

        tags['original_host'] = re.sub(disallow, '_', item_host)
        # tags['original_item_key'] = re.sub(disallow, '_', item_key)
        if row[3]:
            tags['proxy'] = row[3]
        tags['collector'] = 'zabbix'

        # logging.debug("%s\n" % (metric))
        # metric = re.sub(disallow, '_', metric)
        hostMap['items'][row[0]] = { 'metric': metric, 'tags': tags, '_host': row[1], '_key': row[2] }
        # logging.debug("%s\n" % (hostmap[row[0]]) )

    cur.close()
    conn.close()

    hostMap['lastBuildTime'] = time.time()
    print "tcollector.zabbix_bridge.hostMap.time %d %f collector=zabbix action=build host=%s\n" % (time.time(), time.time(), socket.getfqdn())
    print "tcollector.zabbix_bridge.hostMap.duration %d %f collector=zabbix action=build host=%s\n" % (time.time(), time.time() - startTime, socket.getfqdn())
    print "tcollector.zabbix_bridge.hostMap.item.count %d %f collector=zabbix host=%s\n" % (time.time(), len(hostMap['items']), socket.getfqdn())

    logging.info("Finished building HostMap in %d seconds..." % (time.time() - startTime))
    logging.info("Writing HostMap to disk...")
    dumpDict("/tmp/tcollector.zabbix_bridge.host_map.log", hostMap);
    return hostMap



def main():
    utils.drop_privileges()
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1

    nextBuildTime = 0;
    while True:
        if nextBuildTime < time.time():
            hostMap = buildHostMap(settings)
            nextBuildTime = time.time() + settings['hostMapInterval']

        if 'items' in hostMap:
            doZabbixStream(hostMap['items'])
        else:
            logging.error('invalid hostMap: could not find any items')
        logging.info("Waiting for stuff to do...")



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
