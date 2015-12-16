#!/usr/bin/python2

"""A collector to gather statistics from Rollbar (https://rollbar.com)

For now, set your access tokens via environment variable: ROLLBAR_TOKEN
To watch more than one project, separate each project access token with
a space.
"""

import json
import os
import sys
import time
import urllib
import urllib2
import re

from collectors.lib import utils

def dbg(s):
	sys.stderr.write(s+"\n")

def scrub_metric_name(s):
	return re.sub(r'[^a-zA-Z0-9_\.\-]', '', s)

def main():
	tokens_packed = os.environ["ROLLBAR_TOKEN"]

	utils.drop_privileges()
	sys.stdin.close()

	interval = 15

	if not tokens_packed:
		raise ValueError("ROLLBAR_TOKEN envoinment variable not found")

	projects = []

	for token in tokens_packed.split(" "):
		project_info = project_call(token)
		project_info["token"] = token
		project_info["envs"] = {}
		projects.append(project_info)

	# get the most recent occurance ID number, so we can then check every 15 seconds for any new ones since.
	for p in projects:
		first_list = instances_call(p["token"], 1)

		since_id = first_list[0]["id"]
		if not since_id:
			raise RuntimeError("Instance without ID returned")

		p["since_id"] = since_id

	while True:
		time.sleep(interval)

		ts = int(time.time())

		for p in projects:

			p_metric = scrub_metric_name(p["name"])

			page = 1
			new_since = ""
			env_counts = {}
			for e in p["envs"]:
				env_counts[e] = 0

			more = True
			while more:
				l = instances_call(p["token"], page)
				for instance in l:
					if not instance["id"]:
						raise RuntimeError("Instance without ID returned")

					if new_since == "":
						new_since = instance["id"]

					if instance["id"] <= p["since_id"]:
						p["since_id"] = new_since
						more = False
						break

					e = instance["data"].get("environment", "")
					if not e:
						e = "unknown"

					if not env_counts.get(e, 0):
						env_counts[e] = 1
						p["envs"][e] = True
					else:
						env_counts[e] += 1

				if page > 99:
					dbg("Whoa, wayy too much stuff coming back (" + page + " pages)")
					more = False
					break

				page += 1

			for e in env_counts:
				e_metric = scrub_metric_name(e)
				print("rollbar.occurences %d %d env=%s project=%s" % (ts, env_counts[e], e_metric, p_metric))

		sys.stdout.flush()


def instances_call(token, page):
	args = {
		"access_token": token,
		"page": page,
	}

	res = rollbar_call("/api/1/instances/", args)

	if not res["instances"]:
		raise RuntimeError("No result \"instances\" returned")

	return res["instances"]

def project_call(token):
	args = {
		"access_token": token,
	}

	return rollbar_call("/api/1/project/", args)

def rollbar_call(uri, args):
	url = "https://api.rollbar.com" + uri + "?" + urllib.urlencode(args)

	#dbg(url)

	req = urllib2.urlopen(url)
	obj = json.loads(req.read())
	req.close()

	if obj["err"]:
		raise RuntimeError(obj["err"])

	if not obj["result"]:
		raise RuntimeError("No \"result\" returned")

	return obj["result"]

if __name__ == "__main__":
	sys.exit(main())
