#!/usr/bin/env python3

import psycopg2

# var types
# 0 - str
# 1 - int
# 2 - bool

class BHDB:
	def __init__(self, host, login, passwd):
		self.login = login
		self.passwd = passwd
		self.conn = None

		try:
			self.conn = psycopg2.connect("dbname='bh' host='%s' user='%s' password='%s'" % (host, login, passwd))
			self.cur = self.conn.cursor()

		except:
			return None

	def get_id(self, path):
		self.cur.execute('SELECT var_id FROM simple_storage WHERE lower(path) = %s',
			(path.lower(),))
		rows = self.cur.fetchall()
		if len(rows) != 0 and len(rows[0]) > 0:
			return(rows[0][0]) # rows[0] == var_id
		else:
			return None

	def setv(self, path, value):
		# bool = 0
		# int = 1
		# str = 2
		if type(value) is bool:
			var_type = 0
		elif type(value) is int:
			var_type = 1
		elif type(value) is str:
			var_type = 2
		else:
			print('Unknown type')
		# value has always to be string for db
		value = str(value)

		# check if path already exists
		var_id = self.get_id(path)
		if var_id is None:
			# no path yet
			self.cur.execute('INSERT INTO simple_storage (path, var_type, value) VALUES (%s, %s, %s) RETURNING var_id',
				(path, var_type, value,))
			# get var id (uuid)
			rows = self.cur.fetchall()
			if len(rows) != 0 and len(rows[0]) > 0:
				self.conn.commit()
				return(rows[0][0]) # rows[0] == var_id
			else:
				# must not be here though
				print('error in query, setv()')
				sys.exit(-1)
		else:
			# path exists, can just update var instead of creating it
			self.cur.execute('UPDATE simple_storage SET var_type = %s, value = %s WHERE var_id = %s',
				(var_type, value, var_id,))
		self.conn.commit()
		return(var_id)


	def getv(self, path, def_value = None):
		self.cur.execute('SELECT var_type, value FROM simple_storage WHERE lower(path) = %s',
			(path.lower(),))
		rows = self.cur.fetchall()
		if len(rows) != 0 and len(rows[0]) > 0:
			var_type = rows[0][0]
			value = rows[0][1]
			if var_type == 0:
				value = bool(value)
			elif var_type == 1:
				value = int(value)
			elif var_type == 2:
				value = str(value)
			return(value)
		else:
			if def_value is not None:
				self.setv(path, def_value)
				return(def_value)
			return None

	def getv_by_id(self, var_id):
		self.cur.execute('SELECT var_type, value FROM simple_storage WHERE var_id = %s',
			(var_id,))
		rows = self.cur.fetchall()
		if len(rows) != 0 and len(rows[0]) > 0:
			var_type = rows[0][0]
			value = rows[0][1]
			if var_type == 0:
				value = bool(value)
			elif var_type == 1:
				value = int(value)
			elif var_type == 2:
				value = str(value)
			return(value)
		else:
			print('no var by this id: %s' % var_id)
			return None
