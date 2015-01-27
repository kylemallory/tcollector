#!/usr/bin/python2

"""A collector to gather statistics from Rollbar (https://rollbar.com)

For now, set your access token via environment variable: ROLLBAR_TOKEN
"""

import json
import os
import sys
import time
import urllib
import urllib2

from collectors.lib import utils

TOKEN = os.environ["ROLLBAR_TOKEN"]

def dbg(s):
	sys.stderr.write(s+"\n")

def main():
	utils.drop_privileges()
	sys.stdin.close()

	interval = 15

	if not TOKEN:
		raise ValueError("ROLLBAR_TOKEN envoinment variable not found")

	# get the most recent occurance ID number, so we can then check every 15 seconds for any new ones since.
	first_list = instances(1)

	since_id = first_list[0]["id"]
	if not since_id:
		raise RuntimeError("Instance without ID returned")

	while True:
		time.sleep(interval)

		count = 0
		page = 1
		new_since = ""

		more = True
		while more:
			l = instances(page)
			for instance in l:
				if not instance["id"]:
					raise RuntimeError("Instance without ID returned")

				if new_since == "":
					new_since = instance["id"]

				if instance["id"] == since_id:
					since_id = new_since
					more = False
					break

				count += 1

			if page > 10:
				raise RuntimeError("Whoa, wayy too much stuff coming back")

			page += 1

		ts = int(time.time())
		print("rollbar.occurences.total %d %d" % (ts, count))
		sys.stdout.flush()

def instances(page):
	args = {
		"access_token": TOKEN,
		"page": page,
	}

	url = "https://api.rollbar.com/api/1/instances/?" + urllib.urlencode(args)
	#dbg(url)

	req = urllib2.urlopen(url)
	obj = json.loads(req.read())
	req.close()

	if obj["err"]:
		raise RuntimeError(obj["err"])

	if not obj["result"]:
		raise RuntimeError("No \"result\" returned")

	if not obj["result"]["instances"]:
		raise RuntimeError("No result \"instances\" returned")

	return obj["result"]["instances"]


if __name__ == "__main__":
	sys.exit(main())
