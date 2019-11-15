# coding: utf-8

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
import datetime
import re
from dateutil import parser
import openpyxl
from werkzeug.security import generate_password_hash

from app import db
from app import get_db_cursor
from account import Account
from account_grid_id import AccountGridId
from util import read_csv_file
from util import safe_commit


# xlsx_file = open("allcampuses.xlsx", "rb")
# workbook = openpyxl.load_workbook(xlsx_file)
# print "\n".join([tab.replace("JR1 ", "") for tab in workbook.sheetnames])

def import_grid_ids():
    rows = read_csv_file("/Users/hpiwowar/Downloads/SUNYgridids.csv")
    for row in rows:
        print row["username"], row["grid_id"]

        new_account = Account()
        new_account.username = row["username"]
        new_account.password_hash = generate_password_hash(row["username"])
        new_account.display_name = row["campus"]
        new_account.is_consortium = True
        new_account.consortium_id = "93YfzkaA"

        new_grid_id_object = AccountGridId()
        new_grid_id_object.grid_id = row["grid_id"]
        new_account.grid_id_objects.append(new_grid_id_object)

        db.session.add(new_account)

        safe_commit(db)

if __name__ == "__main__":
    import_grid_ids()




