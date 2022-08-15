import datetime
from app import get_db_cursor
from package import Package
from psycopg2 import sql
from psycopg2.extras import execute_values

def publisher_switch(publisher):
	if publisher == "Elsevier":
		return "Elsevier"
	elif publisher == "SpringerNature":
		return "Springer Nature"
	elif publisher == "Wiley":
		return "Wiley"
	elif publisher == "Sage":
		return "SAGE"
	elif publisher == "TaylorFrancis":
		return "Taylor & Francis"
	return publisher

# in database, create table if not done yet
# create table jump_journal_prices_pre_public_price_add as (select * from jump_journal_prices limit 1);
# truncate jump_journal_prices_pre_public_price_add;

# get list of package ids
with get_db_cursor() as cursor:
	qry = """
		select package_id from jump_account_package 
		where not is_deleted
		and currency is not null
		and institution_id in (select distinct(institution_id) from jump_debug_combo_view)
	"""
	cursor.execute(qry)
	pkg_rows = cursor.fetchall()

pkg_ids = [w[0] for w in pkg_rows]
len(pkg_ids)

# remove those already done
with get_db_cursor() as cursor:
	qry = "select DISTINCT(package_id) from jump_journal_prices where public_price;"
	cursor.execute(qry)
	pdone = cursor.fetchall()

pkg_already_done = [w[0] for w in pdone]
len(pkg_already_done)

pkg_ids = list(set(pkg_ids) - set(pkg_already_done))
len(pkg_ids)

# package_id = pkg_ids[0]
# package_id = 'package-jiscelsnr2ind'
for package_id in pkg_ids:
	print(f"({package_id})")
	pkg = Package.query.get(package_id)
	publisher = pkg.publisher
	currency = pkg.currency

	# get public price data
	print(f"  ({package_id}) getting public price data")
	with get_db_cursor() as cursor:
		qry = """
			select %s as package_id,publisher,title,issn_l,null as subject,subscription_price_{} as price,null as year,sysdate as created,true as public_price
			from openalex_computed 
			where publisher ilike %s
			and subscription_price_{} is not null
		""".format(currency.lower(), currency.lower())
		cursor.execute(qry, (package_id, publisher_switch(publisher),))
		public_price_rows = cursor.fetchall()

	# get package price data
	print(f"  ({package_id}) getting custom price data")
	with get_db_cursor() as cursor:
		qry = "select * from jump_journal_prices where package_id = %s"
		cursor.execute(qry, (package_id,))
		price_rows = cursor.fetchall()

	# filter issns in price_rows out of public_price_rows
	if price_rows:
		public_price_rows_to_add = list(filter(lambda w: w['issn_l'] not in [x['issn_l'] for x in price_rows], public_price_rows))
	else:
		print(f"  ({package_id}) no custom prices found")
		public_price_rows_to_add = public_price_rows

	# combine custom and public prices
	price_all_rows = price_rows + public_price_rows_to_add
	# len(price_all_rows)

	# put current prices into a backup table to retrieve later if needed
	print(f"  ({package_id}) backing up prices")
	with get_db_cursor() as cursor:
		qry_backup = "insert into jump_journal_prices_pre_public_price_add (select * from jump_journal_prices where package_id = %s)"
		cursor.execute(qry_backup, (package_id,))
		
	# delete current prices for pkg
	print(f"  ({package_id}) deleting old prices from jump_journal_prices")
	with get_db_cursor() as cursor:
		qry_delete = "delete from jump_journal_prices where package_id = %s"
		cursor.execute(qry_delete, (package_id,))

	# insert new prices for pkg
	print(f"  ({package_id}) inserting new prices {len(price_all_rows)} into jump_journal_prices")
	cols = list(price_all_rows[0].keys())
	with get_db_cursor() as cursor:
		qry = sql.SQL(
		    "INSERT INTO jump_journal_prices ({}) VALUES %s"
		).format(sql.SQL(", ").join(map(sql.Identifier, cols)))
		execute_values(cursor, qry, price_all_rows, page_size=500)

	# update jump_raw_file_upload_object row for price upload
	with get_db_cursor() as cursor:
		qry_exists = "select count(*) from jump_raw_file_upload_object where package_id = %s and file = 'price'"
		cursor.execute(qry_exists, (package_id,))
		file_upload = cursor.fetchone()
		
	if not file_upload[0]:
		print(f"  ({package_id}) no price entry in jump_raw_file_upload_object - creating")
		with get_db_cursor() as cursor:
			file_upload_cols = ['package_id','file','bucket_name','object_name','created','num_rows']
			qry = sql.SQL(
			    "INSERT INTO jump_raw_file_upload_object ({}) VALUES %s"
			).format(sql.SQL(", ").join(map(sql.Identifier, file_upload_cols)))
			cursor.execute(qry, [(package_id, "price", "unsub-file-uploads", f"{package_id}_price.csv", datetime.datetime.utcnow(), len(price_all_rows),)])
	else:
		print(f"  ({package_id}) price entry found in jump_raw_file_upload_object - updating")
		with get_db_cursor() as cursor:
			qry = """
				UPDATE jump_raw_file_upload_object
				SET num_rows=%s, created=sysdate
				where package_id = %s
				and file = 'price'
			"""
			# print(cursor.mogrify(qry, (len(price_all_rows), package_id,)))
			cursor.execute(qry, (len(price_all_rows), package_id,))

	print("\n")
