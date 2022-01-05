import pytest
from counter import Counter
from flask_sqlalchemy import BaseQuery

package_id = "package-iQF8sFiRY99t" # account: scott+test@ourresearch.org
report_name = "trj3"

def test_counter_query():
	assert Counter.__tablename__ == "jump_counter"

	resp = Counter.query.filter(Counter.package_id == package_id, Counter.report_name == report_name)
	assert isinstance(resp, BaseQuery)

	assert resp.limit(4).count() == 4
	assert resp.count() > 20000
