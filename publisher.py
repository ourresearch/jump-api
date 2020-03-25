from sqlalchemy import ForeignKey

from app import db


class Publisher(db.Model):
    __tablename__ = 'jump_publisher'
    id = db.Column(db.Integer, primary_key=True)
    old_package_id = db.Column(db.Text)
    institution_id = db.Column(db.Text, ForeignKey('Institution.id'))
    publisher_name = db.Column(db.Text)
    name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    consortium_publisher_id = db.Column(db.Text, ForeignKey('Publisher.id'))
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

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.name)

