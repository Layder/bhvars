#!/usr/bin/env python3

from BHDB import BHDB

import sys
import time
import config

db = BHDB(config.host, config.login, config.password)

if db.conn is None:
	print('Unable to connect to the database.')
	sys.exit(-1)

db.setv('/test', 100)
print(db.get_id('/test'))
var_id = db.get_id('/test')
print(db.getv_by_id(var_id))
print(db.getv('/test'))
