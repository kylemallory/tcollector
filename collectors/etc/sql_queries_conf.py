#!/usr/bin/env python

def get_settings():
    return {
       'loglevel': 'DEBUG',
       'logfile': '/var/log/sql_collector.log',
       'databases': [{
            'driver': 'postgresql',
            'settings': {
                'host': '127.0.0.1',
                # 'port': '5432',
                'user': 'console',
                'password': 'SJo3mZo7mvHCg1U9PhXL',
                'database': 'console'
            },
            'queries': [
            {
                # this query queries individual hosts for an active state, with the status tag being the status of the last known run
                # this is the inverse of the following query, as it just literally inverts the value of state
                # --- be careful, originally this was used to 'count' the number of active hosts by type, but OpenTSDB does some funky aggregation when the status changes.
                #     It's probably not the RIGHT way to use tags...
                'sql': "select name as host, status, (CASE WHEN updated_at >= ((NOW() AT TIME ZONE 'GMT') - INTERVAL '1 HOUR') THEN '1' ELSE '0' END) as state from nodes where hidden='f' order by state, host",
                'metric': 'puppet.node.active',
                'tags': {
                    'host': '{host}',
                    'status': '{status}'
                },
                'value': '{state}'
            },{
                # this query queries individual hosts for an inactive state, with the status tag being the status of the last known run  this can be
                # this is the inverse of the following query, as it just literally inverts the value of state
                # --- be careful, originally this was used to 'count' the number of active hosts by type, but OpenTSDB does some funky aggregation when the status changes.
                #     It's probably not the RIGHT way to use tags...
                'sql': "select name as host, status, (CASE WHEN updated_at < ((NOW() AT TIME ZONE 'GMT') - INTERVAL '1 HOUR') THEN '1' ELSE '0' END) as state from nodes where hidden='f' order by state, host",
                'metric': 'puppet.node.unresponsive',
                'tags': {
                    'host': '{host}',
                    'last-known-status': '{status}'
                },
                'value': '{state}'
            },{
                # this query finds the actual number of puppet nodes at each status, and counts them.  This is a bit more robust that the above queries, but has another
                # problem, in that if there are no hosts with a particular status, that status/count won't be returned (as opposed to writing '$status = 0')
                'sql': "select status, count(status) from nodes where updated_at >=((NOW() AT TIME ZONE 'GMT') - INTERVAL '1 HOUR') and hidden='f' group by status;",
                'metric': 'puppet.nodes.{status}',
                'tags': {
                },
                'value': '{count}'
            },{
                # this query gathers all of the stats for a particular puppet run, and puts it into OpenTSDB.
                # problem, in that if there are no hosts with a particular status, that status/count won't be returned (as opposed to writing '$status = 0')
                'sql': "select n.name as host, m.category as category, m.name as name, m.value as value from metrics m JOIN nodes n ON report_id = last_apply_report_id",
                'metric': 'puppet.catalog.run.{category}',
                'tags': {
                    'host': '{host}',
                    'item': '{name}'
                },
                'value': '{value}'
            }]
        }]
    }
