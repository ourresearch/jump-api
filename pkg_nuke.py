# Cleanup all data in the database for a given package ID

from app import get_db_cursor

ids=['package-Tasb867CNn3k',
'package-8qQnSQDX8jVM',
'package-XZTkFHmijXwW',
'package-kGUAGddczhyo',
'package-mrWW4q38sxob',
'package-gJ4v8kKWkog6',
'package-EcDrVPuM2gGr',
'package-MjxjsE258G52',
'package-YsBK3YKaZn2D',
'package-VQRZYARhQfmL',
'package-TZjcUAK7wsFm',
'package-XqkvG2DuYGAY',
'package-NGQb3bVY2A7V',
'package-apzp68vt4CEd',
'package-aCG6o8Thxat9',
'package-JRujsLBKYdvt',
'package-4cVJUoYow6Rq',
'package-BbihhozYNNZS',
'package-hF5gGp5ro93T',
'package-hExr5E7hNbiA',
'package-62svwasig5SW',
'package-ZVAuiz26Cmez',
'package-Sa59u2MtEh8j',
'package-9aXbdDF8AwjW',]

def pkg_exists(pid):
	with get_db_cursor() as cursor:
		cmd = f"select * from jump_account_package where package_id = %s"
		cursor.execute(cmd, (pid,))
		row = cursor.fetchone()
	return row

def delete_package(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_account_package where package_id = %s"
		cursor.execute(cmd, (pid,))

def delete_counter(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_counter where package_id = %s"
		cursor.execute(cmd, (pid,))
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_counter_input where package_id = %s"
		cursor.execute(cmd, (pid,))

def delete_pta(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_perpetual_access where package_id = %s"
		cursor.execute(cmd, (pid,))
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_perpetual_access_input where package_id = %s"
		cursor.execute(cmd, (pid,))

def delete_prices(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_journal_prices where package_id = %s"
		cursor.execute(cmd, (pid,))
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_journal_prices_input where package_id = %s"
		cursor.execute(cmd, (pid,))

def delete_uploads(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_raw_file_upload_object where package_id = %s"
		cursor.execute(cmd, (pid,))
	
def delete_apc(pid):
	with get_db_cursor() as cursor:
		cmd = f"delete from jump_apc_authorships where package_id = %s"
		cursor.execute(cmd, (pid,))

for pkg_id in ids:
	if pkg_exists(pkg_id):
		print(f"cleaning up {pkg_id}")
		delete_package(pkg_id)
		delete_counter(pkg_id)
		delete_pta(pkg_id)
		delete_prices(pkg_id)
		delete_uploads(pkg_id)
		delete_apc(pkg_id)
