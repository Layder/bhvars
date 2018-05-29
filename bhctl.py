#!/usr/bin/env python3

from BHDB import BHDB

import sys
import time
import config

db = BHDB(config.host, config.login, config.password)

if db.conn is None:
	print('Unable to connect to the database.')
	sys.exit(-1)

l = len(sys.argv)
if l == 1:
	sys.exit(0)

if l == 2:
	val = db.get_s(sys.argv[1])
	print(val)
	sys.exit(0)

if l == 3:
	var = sys.argv[1]
	val = sys.argv[2]
	db.set_s(var, val)
	sys.exit(0)

