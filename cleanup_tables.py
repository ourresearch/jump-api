# coding: utf-8

import re
from app import get_db_cursor

if __name__ == "__main__":
    table_names = """
        jump_account_package
        jump_apc_authorships
        jump_counter
        jump_counter_input
        jump_journal_prices
        jump_journal_prices_input
        jump_package_scenario
        jump_perpetual_access
        jump_perpetual_access_input
        jump_raw_file_upload_object
        jump_scenario_computed
        jump_scenario_details_paid
        jump_user_institution_permission
        jump_user_institution_permission
        jump_oa_all_vars
        jump_oa_no_submitted_no_bronze
        jump_oa_no_submitted_with_bronze
        jump_oa_with_submitted_no_bronze
        jump_oa_with_submitted_with_bronze
        jump_authorship
        jump_citing
        jump_institution
        ricks_affiliation
        journalsdb_computed
        jump_consortium_members
        permissions_input
    """.split()

    show_tables = """SELECT DISTINCT tablename
        FROM pg_table_def
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """

    with get_db_cursor() as cursor:
        cursor.execute(show_tables)
        rows = cursor.fetchall()

    # make a list of str
    tables = [w[0] for w in rows]
    # filter to tables in table_names
    tables = list(filter(lambda x: re.match("|".join(table_names), x), tables))
    # filter to likely backup tables
    likely_backups = [table for table in tables if table not in table_names]

    print("{} tables\n".format(len(likely_backups)))

    for table in likely_backups:
        print(f"drop table {table};") 
