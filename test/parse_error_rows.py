import simplejson as json
import re

from util import read_csv_file

error_rows = read_csv_file("/Users/hpiwowar/Downloads/jump_file_import_error_rows.csv")

rows = [d for d in error_rows if d["file"]=="price"]
each_error = []
i = 0
for row in rows:
    hits = re.findall('{"ri(.*?)wrong_publisher(.*?)}', row["errors"])
    if hits:
        print hits[0]
    print len(hits)
    print i
    i = i+1


# unknown_issn
# wrong_publisher