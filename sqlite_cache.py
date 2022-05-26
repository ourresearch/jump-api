import argparse
import os
import io
from contextlib import closing
import sqlite3
import zipfile
from app import get_db_cursor
from app import s3_client

class SqLite():
	"""
	SqLite class

	x = SqLite("foobar.db", "sqlitecache.zip")
	x.exists()
	x.create()
	x.exists()
	x.zip_compress()
	x.zip_extract()

	x.create_embargo()
	x.create_unpaywall_downloads()
	x.create_social_networks()
	x.create_oa_recent()
	x.create_oa()
	x.create_society()
	x.create_num_papers()
	"""
	def __init__(self, file, zip_file):
		super(SqLite, self).__init__()
		self.file = file
		self.zip_file = zip_file
	
	def connection(self):
		con = sqlite3.connect(self.file)
		con.row_factory = sqlite3.Row
		return con

	def exists(self):
		try:
			qry = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'"
			rows = self.select(qry)
			rows = [w[0] for w in rows]
			return os.path.isfile(self.file) and len(rows) == 13
		except:
			return False

	def zip_compress(self):
		with zipfile.ZipFile(self.zip_file, "w", zipfile.ZIP_LZMA) as archive:
			archive.write(self.file)

	def zip_extract(self):
		with zipfile.ZipFile(self.zip_file, mode="r") as archive:
			for file in archive.namelist():
				archive.extract(file)

	def query(self, query):
		with closing(self.connection()) as conn:
			with conn:
				with closing(conn.cursor()) as cursor:
					cursor.execute(query)

	def drop_table(self, table):
		self.query(f"DROP TABLE IF EXISTS {table}")

	def create_table(self, table, columns):
		self.query(f"DROP TABLE IF EXISTS {table}")
		self.query(f"CREATE TABLE {table}({columns})")

	def insert(self, query, table, data):
		with closing(self.connection()) as conn:
			with conn:
				with closing(conn.cursor()) as cursor:
					cursor.executemany(query % table, data)

	def select(self, query, ids = None):
		with closing(self.connection()) as conn:
			with conn:
				with closing(conn.cursor()) as cursor:
					if ids:
						cursor.execute(query, ids)
					else:
						cursor.execute(query)
					rows = cursor.fetchall()
		return rows

	def pull(self, query):
		with get_db_cursor() as cursor:
			cursor.execute(query)
			rows = cursor.fetchall()
		return rows

	def pull_embargo_data(self):
		rows = self.pull("select issn_l, embargo from journal_delayed_oa_active")
		for x in rows:
			x[1] = float(x[1])
		return rows

	def pull_unpaywall_downloads(self):
		qry = "select issn_l, num_papers_2018, downloads_total, downloads_0y, downloads_1y, downloads_2y, downloads_3y, downloads_4y from jump_unpaywall_downloads"
		return self.pull(qry)

	def pull_social_networks(self):
		return self.pull("select issn_l, asn_only_rate::float from jump_mturk_asn_rates")

	def pull_oa_recent_data(self):
		out = {}
		for submitted in ["with_submitted", "no_submitted"]:
			for bronze in ["with_bronze", "no_bronze"]:
				table = f"jump_oa_recent_{submitted}_{bronze}_precovid"
				qry = f"select * from {table}"
				out[table] = self.pull(qry)
		return out

	def pull_oa_data(self):
		out = {}
		for submitted in ["with_submitted", "no_submitted"]:
			for bronze in ["with_bronze", "no_bronze"]:
				table = f"jump_oa_{submitted}_{bronze}_precovid"
				qry = f"select * from {table} where year_int >= 2015"
				rows = self.pull(qry)
				for x in rows:
					x['year_int'] = int(x['year_int'])
				out[table] = rows
		return out

	def pull_society(self):
		qry = """
			select issn_l, is_society_journal from jump_society_journals_input 
			where is_society_journal is not null
		"""
		return self.pull(qry)

	def pull_num_papers(self):
		return self.pull("select * from jump_num_papers")

	def create_embargo(self):
		data = self.pull_embargo_data()
		table = "journal_delayed_oa_active"
		self.create_table(table, "issn_l TEXT, embargo REAL")
		self.insert("INSERT INTO %s VALUES (?,?)", table, data)
		self.query(f"CREATE INDEX {table}_issn_l ON {table} (issn_l)")

	def create_unpaywall_downloads(self):
		data = self.pull_unpaywall_downloads()
		table = "jump_unpaywall_downloads"
		self.create_table(table, "issn_l TEXT, num_papers_2018 INTEGER, downloads_total INTEGER, downloads_0y INTEGER, downloads_1y INTEGER, downloads_2y INTEGER, downloads_3y INTEGER, downloads_4y INTEGER")
		self.insert("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)", table, data)
		self.query(f"CREATE INDEX {table}_issn_l ON {table} (issn_l)")

	def create_social_networks(self):
		data = self.pull_social_networks()
		table = "jump_mturk_asn_rates"
		self.create_table(table, "issn_l TEXT, asn_only_rate REAL")
		self.insert("INSERT INTO %s VALUES (?,?)", table, data)
		self.query(f"CREATE INDEX {table}_issn_l ON {table} (issn_l)")

	def create_oa_recent(self):
		data = self.pull_oa_recent_data()
		for key, value in data.items():
			self.create_table(key, "issn_l TEXT, fresh_oa_status TEXT, count INTEGER, publisher TEXT")
			self.insert("INSERT INTO %s VALUES (?,?,?,?)", key, value)
			self.query(f"CREATE INDEX {key}_issn_l ON {key} (issn_l)")

	def create_oa(self):
		data = self.pull_oa_data()
		for key, value in data.items():
			self.create_table(key, "issn_l TEXT, fresh_oa_status TEXT, year_int INTEGER, count INTEGER, publisher TEXT")
			self.insert("INSERT INTO %s VALUES (?,?,?,?,?)", key, value)
			self.query(f"CREATE INDEX {key}_issn_l ON {key} (issn_l)")

	def create_society(self):
		data = self.pull_society()
		table = "jump_society_journals_input"
		self.create_table(table, "issn_l TEXT, is_society_journal TEXT")
		self.insert("INSERT INTO %s VALUES (?,?)", table, data)
		self.query(f"CREATE INDEX {table}_issn_l ON {table} (issn_l)")

	def create_num_papers(self):
		data = self.pull_num_papers()
		table = "jump_num_papers"
		self.create_table(table, "issn_l TEXT, year INTEGER, num_papers INTEGER")
		self.insert("INSERT INTO %s VALUES (?,?,?)", table, data)
		self.query(f"CREATE INDEX {table}_issn_l ON {table} (issn_l)")

	def create(self):
		self.create_embargo()
		self.create_unpaywall_downloads()
		self.create_social_networks()
		self.create_oa_recent()
		self.create_oa()
		self.create_society()
		self.create_num_papers()

	def s3_fetch(self):
		s3_client.download_file(Bucket="unsub-cache", Key=self.zip_file, Filename=self.zip_file)

	def s3_upload(self):
		s3_client.upload_file(Filename=self.zip_file, Bucket="unsub-cache", Key=self.zip_file)


# python sqlite_cache.py --run
# heroku local:run python sqlite_cache.py --run
# heroku run --size=performance-l python sqlite_cache.py --run -r heroku
if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("--run", help="Prepare SqLite database and upload to S3", action="store_true", default=False)
	parsed_args = parser.parse_args()

	if parsed_args.run:
		x = SqLite(os.getenv("SQLITE_PATH", "sqlite_cache.db"), "sqlitecache.zip")
		x.create()
		x.zip_compress()
		x.s3_upload()


# sqlite_con = sqlite3.connect("commondata.db")
# sqlite_con.row_factory = sqlite3.Row

# @contextmanager
# def sqlite_connection():
# 	try:
# 	    con = sqlite3.connect("commondata.db")
# 	    con.row_factory = sqlite3.Row
# 	    yield con
# 	finally:
# 		con.close()

# @contextmanager
# def sqlite_cursor():
# 	with sqlite_connection() as con:
# 		try:
# 		    cursor = con.cursor()
# 		    yield cursor
# 		except sqlite3.OperationalError as e:
# 	            raise Exception("Error: error in sqlite_cursor: {}".format(e))
#         finally:
#             cursor.close()
#             pass

# def sqlite_connection():
#     con = sqlite3.connect("commondata.db")
#     con.row_factory = sqlite3.Row
#     return con

# def foobar(data):
	# with closing(sqlite_connection()) as conn:
	# 	with conn:
	# 		with closing(conn.cursor()) as cursor:
	# 			cursor.execute(f"DROP TABLE IF EXISTS {table}")
# 				cursor.executemany("insert into cheese values (?, ?)", data)
# 				# cursor.execute("CREATE TABLE jump_num_papers(issn_l, year, num_papers)")
# 				# cursor.executemany("INSERT INTO jump_num_papers(issn_l, year, num_papers) VALUES (?,?,?)", rows)
# 				# cursor.execute("CREATE INDEX jump_num_papers_issn_l ON jump_num_papers (issn_l)")



# con = sqlite3.connect("commondata.db")
# con.row_factory = sqlite3.Row
# cursor = con.cursor()

# table = "cheese"
# table = "journal_delayed_oa_active"
# data = [('a',5),('b',6),]
# len(data)
# list(data[0].keys())
# list(data[0])
# type(data[0])
# type(data[0][1])
# columns = ['issn_l', 'embargo']

# def sqlite_query(query):
# 	with closing(sqlite_connection()) as conn:
# 		with conn:
# 			with closing(conn.cursor()) as cursor:
# 				cursor.execute(query)

# def sqlite_create_table(table, columns):
# 	sqlite_query(f"DROP TABLE IF EXISTS {table}")
# 	sqlite_query(f"CREATE TABLE {table}({columns})")
# 	# sqlite_query(f"CREATE TABLE {table}({','.join(columns)})")

# def sqlite_insert(query, table, data):
# 	with closing(sqlite_connection()) as conn:
# 		with conn:
# 			with closing(conn.cursor()) as cursor:
# 				cursor.executemany(query % table, data)

# def sqlite_create_embargo(data):
# 	# columns = ['issn_l', 'embargo']
# 	sqlite_create_table("journal_delayed_oa_active", "issn_l TEXT, embargo REAL")
# 	sqlite_insert("INSERT INTO %s VALUES (?,?)", "journal_delayed_oa_active", data)

# def sqlite_select(table):
# 	with closing(sqlite_connection()) as conn:
# 		with conn:
# 			with closing(conn.cursor()) as cursor:
# 				cursor.execute("select * from %s" % table)
# 				rows = cursor.fetchall()
# 	return rows

# def sqlite_fetch_cached():
# 	data = {}
# 	data['embargo_dict'] = sqlite_select("journal_delayed_oa_active")
# 	return data

# sqlite_create_embargo(data)
# sqlite_fetch_cached()

# with zipfile.ZipFile("sqlite_cache_bzip2.zip", "w", zipfile.ZIP_BZIP2) as archive:
# 	archive.write("sqlite_cache.db")

# with zipfile.ZipFile("sqlite_cache_lzma.zip", "w", zipfile.ZIP_LZMA) as archive:
# 	archive.write("sqlite_cache.db")

# with zipfile.ZipFile("sqlite_cache_lzma.zip", mode="r") as archive:
# 	for file in archive.namelist():
# 	    archive.extract(file)

# # tarfile
# import tarfile
# ## write, lzma
# with tarfile.open("sqlite_cache_bzip2.tar.lzma", "w:xz") as tar:
#     tar.add("sqlite_cache.db")

# ## read, lzma
# with tarfile.open(archive, "r:gz") as tar:
#     member = tar.getmember("words3.txt")
#     if member.isfile():
#         tar.extract(member, "/tmp/")
