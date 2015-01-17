#!/usr/bin/env python
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

def get_settings():
    altItemMappings = OrderedDict([
        ('jmx', {
            'argParser': 'jmxParser', # which parser to use for this item's parameters
            'flags': { # parameters to pass to the argParder
                'expandParameters': True, # don't remember what this was for
                'splitExpression': ',', # a regex expression passed to regex.split() that is used to split the parameters; defaults to ','
                'paramPrefix': 'jmx', # the name used to store the parameters, ie, 'jmx'.  Parameters are then expanded using {jmx[index]} or {jmx.name}
                'namedParameters': True
            },
            'metric': 'jmx.{1}.{jmx:1}',
            'tags': {
                'jmx.domain': '{jmxDomain}',
                'jmx.bean.{jmxBean}': '{value}'
            }
        })
    ])

    hostItemMapping = OrderedDict([
        # key: a regex expression to match a given zabbix item key
        # val: a dict that includes the new metric name, and all tags to be associated with the new metric
        ('jmx-processing-time-per-request-(.+)$', {
            'metric': 'jmx.Catalina.processingTimePerRequest',
            'tags': {
                'application': '{1}'
            }
        }),
        ('jmx\.solr\/([^\.]+)\.(.+)$', {
            'metric': 'jmx.solr.{2}',
            'tags': {
                'type': '{1}'
            }
        }),
        ('ProcesserLoad\.\[([^\]]+)\]', {
            'metric': 'vcenter.proc.load',
            'tags': {
                'cpu': '{1}'
            }
        }),
        ('StorageSize\.\[([^\]]+)\]', {
            'metric': 'vcenter.storage.size',
            'tags': {
                'device': '{1}'
            }
        }),
        ('StorageUsed\.\[([^\]]+)\]', {
            'metric': 'vcenter.storage.used',
            'tags': {
                'device': '{1}'
            }
        }),
        ('StorageUsagePercent\.\[([^\]]+)\]', {
            'metric': 'vcenter.storage.percentUsed',
            'tags': {
                'device': '{1}'
            }
        }),
        ('StorageAllocationFailures\.\[([^\]]+)\]', {
            'metric': 'vcenter.storage.allocation-failures',
            'tags': {
                'device': '{1}'
            }
        }),
        ('DiskStatus\.\[\"(\d+)\"\]', {
            'metric': 'nas.disk.status',
            'tags': {
                'disk': '{1}'
            }
        }),
        ('apache\[localhost,([^\]]+)\]', {
            'metric': 'apache.{1}',
        }),
        ('iostat\[([^,]+),([^\]]+)\]', {
            'metric': 'iostat.{2}',
            'tags': {
                'device': '{1}'
            }
        }),
        ('hastat\.(.+)-(BACKEND|FRONTEND)-(.+)', {
            'metric': 'haproxy.{3}',
            'tags': {
                'directon': '{2}',
                'interface': '{1}'
            }
        }),
        ('icmppingsec\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'icmp.ping.responseTime.secs',
            'tags': {
                'target': '{1}',
                'packet-count': '{2}',
                'packet-interval': '{3}',
                'packet-size': '{4}',
                'timeout': '{5}',
                'mode': '{6}'
            }
        }),
        ('icmppingloss\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'icmp.ping.percentLoss',
            'tags': {
                'target': '{1}',
                'packet-count': '{2}',
                'packet-interval': '{3}',
                'packet-size': '{4}',
                'timeout': '{5}'
            }
        }),
        ('icmpping\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'icmp.ping.success',
            'tags': {
                'target': '{1}',
                'packet-count': '{2}',
                'packet-interval': '{3}',
                'packet-size': '{4}',
                'timeout': '{5}'
            }
        }),
        ('log\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'log',
            'tags': {
                'file': '{1}',
                'regex': '{2}',
                'encoding': '{3}',
                'maxlines': '{4}',
                'mode': '{5}'
            }
        }),
        ('logrt\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'logrt',
            'tags': {
                'format': '{1}',
                'regex': '{2}',
                'encoding': '{3}',
                'maxlines': '{4}',
                'mode': '{5}'
            }
        }),
        ('net\.dns(\.record)\[([^,]*),?([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'net.dns{1}',
            'tags': {
                'ip': '{2}',
                'zone': '{3}',
                'type': '{4}',
                'timeout': '{5}',
                'count': '{6}'
            }
        }),
        ('net\.if(\.([^\[]*))\[([^,]+),?([^,]*)\]', {
            # this matches net.if[*] as well as net.if.collisions[*], with the . as part of the first capture group (so don't reprint it in the metric name)
            'metric': 'net.interface.{4}',
            'tags': {
                'type': '{2}',
                'interface': '{3}'
            }
        }),
        ('net\.(tcp|udp)\.listen\[([^,]*)\]', {
            'metric': 'net.{1}.listen',
            'tags': {
                'port': '{2}'
            }
        }),
        ('net\.tcp\.port\[([^,]*),?([^,]+)\]', {
            'metric': 'net.tcp.listen',
            'tags': {
                'ip': '{1}',
                'port': '{2}'
            }
        }),
        ('net\.tcp\.service\[([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'net.tcp.service.available',
            'tags': {
                'service': '{1}',
                'ip': '{2}',
                'port': '{3}'
            }
        }),
        ('net\.tcp\.service.perf\[([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'net.tcp.service.timeToConnect',
            'tags': {
                'service': '{1}',
                'ip': '{2}',
                'port': '{3}'
            }
        }),
        ('proc\.mem\[([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'system.processes.memory',
            'tags': {
                'name': '{1}',
                'user': '{2}',
                'method': '{3}',
                'cmdline': '{4}'
            }
        }),
        ('proc\.num\[([^,]*),?([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'system.processes.count',
            'tags': {
                'name': '{1}',
                'user': '{2}',
                'state': '{3}',
                'cmdline': '{4}'
            }
        }),
        ('system\.cpu\.load\[([^,]*),?([^,]*)\]', {
            'metric': 'system.cpu.load',
            'tags': {
                'cpu': '{1}',
                'sampleInterval': '{2}'
            }
        }),
        ('system\.cpu\.util\[([^,]*),?([^,]*),?([^,]*)\]', {
            'metric': 'system.cpu.util',
            'tags': {
                'cpu': '{1}',
                'type': '{2}',
                'sampleInterval': '{3}'
            }
        }),
        ('system\.swap\.(.+)\[,\s*([^\]]+)\]', {
            'metric': 'system.swap.{1}.{2}',
        }),
        ('vfs\.fs(\.[^\[]+)\[([^,]*),?([^\]]+)\]', {
            'metric': 'vfs{1}.{3}',
            'tags': {
                'fs': '{2}'
            }
        }),
        ('vfs\.dev\.([^\[]+)\[([^,]*),?\s*([^\]]+)\]', {
            'metric': 'vfs.dev.{3}',
            'tags': {
                'operation': '{1}',
                'device': '{2}'
            }
        }),
        ('vfs\.file\.cksum\[([^\]]*)\]', {
            'metric': 'vfs.file.cksum',
            'tags': {
                'file': '{1}'
            }
        }),
        # This is the default, fall-through catch-all
        ('([A-Za-z0-9_\-\.]+)\.*\[,*\"*([^\"\]]+)\"*\]', {
            'metric': '{1}.{2}'
        })
    ])

    # """MySQL replication credentials."""
    # A user with "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT"
    return {
        # DB credentials (see pymysql connection info).
        'mysql': {
            'host': 'dbhost.domain.tld',
            'port': 3306,
            'user': 'username',
            'passwd': 'password',
            'db': 'zabbix'
        },
        'loglevel': 'DEBUG',
        'logfile': '/var/log/zabbix_collector.log',
        'slaveid': 31,                    # Slave identifier, it should be unique.
        'disallow': '[^a-zA-Z0-9\-_\.\/]', # Regex of characters to replace with _.
        'macroMapInterval': 1200,       # How often to reload itemid, hostmap from DB.
        'hostMapInterval': 1200,       # How often to reload itemid, hostmap from DB.
        'ignored-keys': [
        ],
        'ignored-hosts': [
            'Website',
            'localhost'
        ],
        'mappings': {
            # these macros are applied AFTER zabbix macros.
            # the zabbix item-key is modified in-place, so if two host/macros match a given item, the first
            # macros will be applied, which will prevent the second from matching (since {$MACRO} no longer exists in the item-key)
            # This is important behaviour, as the application of zabbix macros will do the same -- TODO: Revisit this design choice
            # -- perhaps we need to build a complete list of all macros for a given host, and let the last matching macro override all others
            'macros': {
                'hostfoo.domain': {
                    '{$APP1_CONTEXT}': 'app1',
                    '{$APP2_CONTEXT}': 'app2',
                    '{$APP3_CONTEXT}': 'app3'
                }
            },
            'item-key': hostItemMapping,
            'item-host': {
                # a regex expression to match a given zabbix host
                # a set of tags to include in the metric when a zabbix host matches the regex
                # 'memberWeb-trust02.xmission-51e.prod': {
                #     'host': 'trust02.xmission-51e.prod',
                #     'location': 'xmission-51e',
                #     'environment': 'xmission-51e.prod',
                #     'class': 'prod'
                # },
                '(9ex-dc)-([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)': { # 9ex-dc-monitordb01.corp.internal
                    'role': '{2}',
                    'host': '{2}.{3}.{4}',
                    'location': '{1}',
                    'environment': '{3}.{4}',
                    'class': '{4}'
                },
                '([A-Za-z0-9\-]+)-([A-Za-z0-9]+)\.([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)': { # memberWeb-trust02.xmission-51e.prod
                    'role': '{1}',
                    'host': '{2}.{3}.{4}',
                    'location': '{3}',
                    'environment': '{3}.{4}',
                    'class': '{4}'
                },
                '([A-Za-z0-9]+)\.([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)': { # memberWeb-trust02.xmission-51e.prod
                    'host': '{1}.{2}.{3}',
                    'environment': '{2}.{3}',
                    'location': '{2}',
                    'class': '{3}'
                }
            }
        }
    }


