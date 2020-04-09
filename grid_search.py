import unicodecsv as csv
from whoosh import index, sorting
from whoosh.analysis import StandardAnalyzer
from whoosh.fields import Schema, STORED, NGRAMWORDS, NUMERIC
from whoosh.qparser import MultifieldParser

_schema = Schema(
    grid=STORED(),
    name=NGRAMWORDS(stored=False),
    aliases=NGRAMWORDS(stored=False),
    num_students=NUMERIC(int, sortable=True, stored=False),
    citation_score=NUMERIC(int, sortable=True, stored=False),
)

_index_path = 'data/grid-whoosh-index'


def _read_grid_csv_rows():
    rows = []
    with open('data/grid-metrics.csv') as grid_csv:
        reader = csv.DictReader(grid_csv)
        for row in reader:
            row['aliases'] = row['aliases'].split(u'###') if row['aliases'] else []
            row['num_students'] = int(row['num_students']) if row['num_students'] else None
            row['citation_score'] = float(row['citation_score']) if row['citation_score'] else None
            rows.append(row)

    return rows


_grid_rows = dict((row['id'], row) for row in _read_grid_csv_rows())


def _create_index():
    idx = index.create_in(_index_path, schema=_schema)
    index_writer = idx.writer()

    for row in _read_grid_csv_rows():
        index_writer.add_document(
            grid=row['id'],
            name=row['name'],
            num_students=row['num_students'] or 0,
            citation_score=int(100 * float(row['citation_score'] or 0.0)),
            aliases=u' '.join(row['aliases']).lower().replace('university', ''),
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
    query_str = u' '.join([t.text for t in _analyzer(query_str) if not 'university'.startswith(t.text)])

    q = _query_parser.parse(query_str)
    return [
        _grid_rows[row['grid']] for row in
        _searcher.search_page(q, 1, results, sortedby=[
            sorting.FieldFacet('citation_score', reverse=True),
            sorting.FieldFacet('num_students', reverse=True),
            sorting.ScoreFacet(),
        ])
    ]
