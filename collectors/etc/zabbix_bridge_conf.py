#!/usr/bin/env python
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

def get_settings():
    hostMappings = OrderedDict([
        # a regex expression to match a given zabbix host
        # a set of tags to include in the metric when a zabbix host matches the regex
        # 'role-host.dc01.domain.tld': {
        #     'host': 'host.dc01.domain.tld',
        #     'location': 'dc01',
        #     'domain': 'domain.tld'
        # },
        # '([A-Za-z0-9\-]+)-([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)\.([A-Za-z0-9-]+)': { # role-host.subdomain.domain.tld
        #     'role': '{1}',
        #     'host': '{2}.{3}.{4}',
        #     'location': '{2}',
        #     'domain': '{3}.{4}',
        #     'fqdn': '{0}'
        # }
        # a regex expression to match a given zabbix host
        # a set of tags to include in the metric when a zabbix host matches the regex
        # 'memberWeb-trust02.xmission-51e.prod': {
        #     'host': 'trust02.xmission-51e.prod',
        #     'location': 'xmission-51e',
        #     'environment': 'xmission-51e.prod',
        #     'class': 'prod'
        # },
        ('(9ex-\w+)-([\w-]+)\.(\w+)\.(\w+)', { # 9ex-dc-monitordb01.corp.internal
            'role': '{2}',
            'host': '{0}',
            'location': '{1}',
            'environment': '{3}.{4}',
            'class': '{4}'
        }),
        ('(dev\w+)(-(\w+))*\.corp.internal', { # dev14-1.corp.internal
            'host': '{0}',
            'location': 'corp',
            'environment': '{1}',
            'class': 'dev'
        }),
        ('(\w+)-(\w+)\.([\w-]+)\.(\w+)', { # memberWeb-trust02.xmission-51e.prod
            'role': '{1}',
            'host': '{2}.{3}.{4}',
            'location': '{3}',
            'environment': '{3}.{4}',
            'class': '{4}'
        }),
        ('(\w+)\.([\w-]+)\.(\w+)', { # trust02.xmission-51e.prod
            'host': '{1}.{2}.{3}',
            'environment': '{2}.{3}',
            'location': '{2}',
            'class': '{3}'
        })
    ])

    itemMappings = OrderedDict([
        # key: a regex expression to match a given zabbix item key
        # val: a dict that includes the new metric name, and all tags to be associated with the new metric

	# haproxy.trap["instance=haproxy-external-trust,end=BACKEND,role=entity-search-xapi::haproxy.bck"]
        ('haproxy\.trap\[\"(.*)::(.*)\"\]', {
            'argString': '{1}', # the string to pass to the argsParser, this is typically a match group from the mapping's regex key
            'argParser': 'named', # which parser to use for this item's args; 'named' parser expects 2 parameters 
            'flags': { 'expandParameters': True },
            'metric': '{2}',
        }),
        ('redis\.trap\[(.*)::(.*)\]', {
            'argString': '{1}', # the string to pass to the argsParser, this is typically a match group from the mapping's regex key
            'argParser': 'named', # which parser to use for this item's args; 'named' parser expects 2 parameters
            'flags': { # parameters to pass to the argParser
                'expandParameters': True, # whether to create tags from named query params:  Ie, "type=Foo,name=bar" expands tags: type=Foo and name=bar
            },
            'metric': '{2}',
            'tags': {} # these are appended to any that the argParser populated (ie, via expandParameters)
        }),
        ('jmx(\[([^\]]*)\])', {
            #  jmx['domain:query','attribute']
            # 'domain', 'query', 'attribute' are always placed into the parameter map, but are only ever explicitly expanded into tags
            'argString': '{2}', # the string to pass to the argsParser, this is typically a match group from the mapping's regex key
            'argParser': 'jmx', # which parser to use for this item's args; 'jmx' parser expects 2 parameters, the first is a query, comprised of a list of name-value pairs, with an optional, proceeding 'domain'; the second is an 'attribute' name.
            'flags': { # parameters to pass to the argParser
                'expandParameters': True, # whether to create tags from jmx MBean query params:  Ie, "domain:type=Foo,name=bar" expands tags: type=Foo and name=bar
                'parameterPrefix': 'jmx.' # a prefix to prepend on all parameters parsed by the parser, ie, 'jmx.'.  Parameters are then expanded using {jmx.type}
            },
            'metric': 'jmx.{@jmx.domain}.{@jmx.attribute}',
            'tags': {} # these are appended to any that the argParser populated (ie, via expandParameters)
        }),
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
        ('hastat\.(dmz|trust)_(.+)-(BACKEND|FRONTEND)-(.+)', {
            'metric': 'haproxy.{4}',
            'tags': {
                'direction': '{3}',
                'interface': '{2}',
                'zone': '{1}'
            }
        }),
        ('icmpping\[([^\]]*)\]', {
            'argParser': 'index',
            'flags': {
                'namedParameters': ['target','packet-count','packet-interval','packet-size','timeout','mode'],
                'expandParameters': True
            },
            'metric': 'icmp.ping.success',
        }),
        ('icmppingsec\[([^\]]*)\]', {
            'argParser': 'index',
            'flags': {
                'namedParameters': ['target','packet-count','packet-interval','packet-size','timeout','mode'],
                'expandParameters': True
            },
            'metric': 'icmp.ping.responseTime.secs',
        }),
        ('icmppingloss\[([^\]]*)\]', {
            'argParser': 'index',
            'flags': {
                'namedParameters': ['target','packet-count','packet-interval','packet-size','timeout','mode'],
                'expandParameters': True
            },
            'metric': 'icmp.ping.percentLoss',
        }),
        ('web.test.([^\[]*)\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'argString': '{2}',
            'flags': {
                'namedParameters': ['scenario','step','resp'],
                'expandParameters': True
            },
            'metric': 'web.test.{1}',
        }),
        ('log\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['file','regex','encoding','maxlines','mode'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'log',
        }),
        ('logrt\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['file','regex','encoding','maxlines','mode'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'log.rt',
        }),
        ('net\.dns(\.record)\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'argString': '{2}',
            'flags': {
                'namedParameters': ['ip','zone','type','timeout','count'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'log',
        }),
        ('net\.if(\.[^\[]*)\[([^,]+),?([^,]*)\]', {
            # this matches net.if[*] as well as net.if.collisions[*], with the . as part of the first capture group (so don't reprint it in the metric name)
            'metric': 'net.interface{1}.{3}',
            'tags': {
                'interface': '{2}'
            }
        }),
        ('net\.(tcp|udp)\.listen\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['port'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'net.{1}.listen',
        }),
        ('net\.tcp\.port\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['ip','port'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'net.tcp.port',
        }),
        ('net\.tcp\.service\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['service','ip','port'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'net.tcp.service',
        }),
        ('net\.tcp\.service.perf\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['service','ip','port'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'net.tcp.service.timeToConnect',
        }),
        ('proc\.mem\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['name','user','method','cmdline'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'system.processes.memory',
        }),
        ('proc\.num\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['name','user','method','cmdline'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'system.processes.count',
        }),
        ('system\.cpu\.load\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['cpu','sampleInterval'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'system.cpu.load',
        }),
        ('system\.cpu\.util\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'flags': {
                'namedParameters': ['cpu','type','sampleInterval'],
                'expandParameters': True # whether to create tags from the parsed params:  Ie, "ifInErrors[eth1]" expands the parameter with the name 'interface' named parameter: interface=eth1
            },
            'metric': 'system.cpu.utilization',
        }),
        ('system\.swap\.(.+)\[,\s*([^\]]+)\]', {
            'metric': 'system.swap.{1}.{2}',
        }),
        ('vfs.fs.([^\[]+)\[([^\]]*)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'argString': '{2}',
            'flags': {
                'namedParameters': ['path','attribute'],
            },
            'metric': 'vfs.fs.{1}.{@attribute}',
            'tags': {
                'fs': '{@path}'
            }
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
        ('(vm\.memory\.size)\[(.+)\]', {
            'metric': 'vm.memory.{2}'
        }),
        # This is the default, fall-through catch-all
        # ('([A-Za-z0-9_\-\.]+)\.*\[,*\"*([^\"\]]+)\"*\]', {
        #     'metric': '{1}.{2}'
        # })
        ('(.+)\[(.+)\]', {
            'argParser': 'index', # which parser to use for this item's parameters
            'argString': '{2}',
            'flags': {
                'expandParameters': True,
                'parameterPrefix': 'param.'
            },
            'metric': '{1}'
        }),
        ('.+', {
            'metric': '{0}'
        })
    ])

    # """MySQL replication credentials."""
    # A user with "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT"
    return {
        # DB credentials (see pymysql connection info).
        'mysql': {
#            'host': 'dbhost.domain.tld',
#            'port': 3306,
#            'user': 'username',
#            'passwd': 'password',
            'host': '9ex-dc-monitordb01.corp.internal',
            'port': 3306,
            'user': 'zabbix_repl',
            'passwd': 'ReplicateZabbix',
            'db': 'zabbix'
        },
        'loglevel': 'INFO',
        'logfile': '/var/log/zabbix_collector.log',
        'slaveid': 21,                    # Slave identifier, it should be unique.
        'disallow': '[^a-zA-Z0-9\-_\.\/]', # Regex of characters to replace with _.
        'macroRefreshInterval': 7200,       # How often (seconds) to reload macros from Zabbix (fast & few)   2 hour
        'itemRefreshInterval': 86400,      # How often (seconds) to reload itemid, hostmap from Zabbix (many and slow)   24 hours
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
            'item-key': itemMappings,
            'item-host': hostMappings
        }
    }


