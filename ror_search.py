# import unicodecsv as csv
import csv
from whoosh import index, sorting
from whoosh.analysis import StandardAnalyzer
from whoosh.fields import Schema, STORED, NGRAMWORDS, NUMERIC
from whoosh.qparser import MultifieldParser

_schema = Schema(
    ror=STORED(),
    grid=STORED(),
    name=NGRAMWORDS(stored=False),
    aliases=NGRAMWORDS(stored=False),
    num_students=NUMERIC(int, sortable=True, stored=False),
    citation_score=NUMERIC(int, sortable=True, stored=False),
)

_index_path = 'data/ror-whoosh-index'


def _read_ror_csv_rows():
    rows = []
    with open('data/ror-metrics.csv') as ror_csv:
        reader = csv.DictReader(ror_csv)
        for row in reader:
            row['aliases'] = row['aliases'].split('###') if row['aliases'] else []
            row['num_students'] = int(row['num_students']) if row['num_students'] else None
            row['citation_score'] = float(row['citation_score']) if row['citation_score'] else None
            rows.append(row)

    return rows


_ror_rows = dict((row['ror_id'], row) for row in _read_ror_csv_rows())


def _create_index():
    idx = index.create_in(_index_path, schema=_schema)
    index_writer = idx.writer()

    for row in _read_ror_csv_rows():
        index_writer.add_document(
            ror=row['ror_id'],
            grid=row['grid_id'],
            name=row['name'],
            num_students=row['num_students'] or 0,
            citation_score=int(100 * float(row['citation_score'] or 0.0)),
            aliases=' '.join(row['aliases']).lower().replace('university', ''),
        )

    index_writer.commit()


if not index.exists_in(_index_path):
    _create_index()

_index = index.open_dir(_index_path)
_searcher = _index.searcher()

_query_parser = MultifieldParser(
    ['name', 'aliases'],
    schema=_index.schema,
    fieldboosts={
        'name': 50,
        'aliases': 1
    }
)

_analyzer = StandardAnalyzer()


def autocomplete(query_str, results=10):
    query_str = ' '.join([t.text for t in _analyzer(query_str) if not 'university'.startswith(t.text)])

    q = _query_parser.parse(query_str)
    return [
        _ror_rows[row['ror']] for row in
        _searcher.search_page(q, 1, results, sortedby=[
            sorting.FieldFacet('citation_score', reverse=True),
            sorting.FieldFacet('num_students', reverse=True),
            sorting.ScoreFacet(),
        ])
    ]
