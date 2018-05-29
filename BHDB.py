#!/usr/bin/env python3

import psycopg2

class BHDB:
	def __init__(self, login, passwd):
		self.login = login
		self.passwd = passwd
		self.conn = None

		try:
			self.conn = psycopg2.connect("dbname='bh' host='%s' user='%s' password='%s'" % (host, login, passwd))
			self.cur = self.conn.cursor()

		except:
			return None

	def set_s(self, path, val):
		path_id = self.get_path_id(path)
		if path_id is not None:
			self.set_by_id(path_id, val)
			return(path_id)

		path_uuid = self.get_path_uuid(path)
		var_name = self.get_var_name(path)

		if path_uuid is None:
			self.set_path_uuid(path)
		self.cur.execute("INSERT INTO storage (ptr_id, var_type, var_name, val_str) VALUES (%s, 0, %s, %s) RETURNING var_id",
			(path_uuid, var_name, val,))
		rows = self.cur.fetchall()
		self.conn.commit()
		if len(rows) != 0 and len(rows[0]) > 0:
			ptr_id = rows[0]
			self.cur.execute("INSERT INTO ptrs (ptr_id, path) VALUES (%s, %s)", (ptr_id, path,))
			self.conn.commit()

	def get_s(self, path):
		path_id = self.get_path_id(path)
		if path_id is not None:
			val = self.get_by_id(path_id)
			return(val)

	def set_by_id(self, path_id, val):
		self.cur.execute("UPDATE storage SET val_str = %s WHERE var_id = %s", (val, path_id,))
		self.conn.commit()
		return None

	def get_by_id(self, path_id):
		self.cur.execute("SELECT val_str FROM storage WHERE var_id = %s", (path_id,))
		rows = self.cur.fetchall()
		return(rows[0][0])

	def get_path_id(self, path):
		self.cur.execute("SELECT ptr_id FROM ptrs WHERE lower(path) = %s", (path,))
		rows = self.cur.fetchall()
		if len(rows) == 0 or len(rows[0]) == 0:
			return None
		else:
			return rows[0][0]

		path_uuid = self.get_path_uuid(path)
		var_name = self.get_var_name(path)

		print('path uuid = %s' % path_uuid)
		print('var name = %s' % var_name)
		self.cur.execute("SELECT var_id FROM storage WHERE ptr_id = %s AND lower(var_name) = %s", (path_uuid, var_name,))
		rows = self.cur.fetchall()
		if len(rows[0] > 0):
			return True
		return False

	def get_var_name(self, path):
		itms = [x.strip() for x in path.split('/')]
		l = len(itms)
		if l < 1:
			# invalid path
			return(None)

		return(itms[l-1])

	def get_path_uuid(self, path):
		itms = [x.strip() for x in path.split('/')]
		l = len(itms)
		if l < 1:
			# invalid path
			return(None)

		# now get prepath
		if l > 1:
			itms = itms[:-1]
			prepath = str.join('/', itms)
			try:
				self.cur.execute("SELECT ptr_id FROM ptrs WHERE path = %s", (prepath,))
				rows = self.cur.fetchall()
				if len(rows) == 1:
					return(rows[0])
			except:
				print('Error fetching ptr_id from "ptrs" table')
				return None
		return(None)

	def set_path_uuid(self, path):
		itms = [x.strip() for x in path.split('/')]
		l = len(itms)
		if l < 1:
			# invalid path
			return(None)

		# now get prepath
		if l > 1:
			itms = itms[:-1]
			prepath = str.join('/', itms)
			try:
				path_id = self.get_path_uuid(prepath)
				if path_id is None:
					self.cur.execute("INSERT INTO ptrs (path) VALUES (%s)", (prepath,))
					self.conn.commit()
					path_id = self.get_path_uuid(prepath)
					return(path_id)
			except:
				print('Error fetching ptr_id from "ptrs" table')
				return None
		return(None)
