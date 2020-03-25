import datetime

import shortuuid
from sqlalchemy import ForeignKey

from app import db


class Publisher(db.Model):
    __tablename__ = 'jump_publisher'
    id = db.Column(db.Integer, primary_key=True)
    old_package_id = db.Column(db.Text)
    institution_id = db.Column(db.Text, ForeignKey('jump_institution.id'))
    publisher_name = db.Column(db.Text)
    name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    consortium_publisher_id = db.Column(db.Text, ForeignKey('jump_publisher.id'))
    is_demo = db.Column(db.Boolean)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'is_demo': self.is_demo,
            'journal_detail': {},  # same as Package.journal_detail
            'scenarios': [
                # {name: 'my groovy scenario', id: 'demo-scenario-123}
            ],
            'data_files': [
                {'name': 'counter', 'uploaded': None},
                {'name': 'perpetual-access', 'uploaded': None},
                {'name': 'prices', 'uploaded': None},
                {'name': 'core-journals', 'uploaded': None},
            ]

        }

    def __init__(self, **kwargs):
        self.id = u'pub-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(Publisher, self).__init__(**kwargs)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.name)

