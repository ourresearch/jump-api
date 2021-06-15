# coding: utf-8

# python backup_tables.py
if __name__ == "__main__":
    suffix = "20210517"
    table_names = """
        jump_counter
        jump_journal_prices
        jump_perpetual_access
        jump_counter_input
        jump_journal_prices_input
        jump_perpetual_access_input
        jump_account_package
        jump_scenario_details_paid
        jump_package_scenario
        jump_scenario_computed
    """.split()

    for table in table_names:
        print u"create table {table}_{suffix} as (select * from {table});".format(table=table, suffix=suffix)