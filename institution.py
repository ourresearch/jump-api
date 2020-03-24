import datetime

import shortuuid

from app import db


class Institution(db.Model):
    __tablename__ = 'jump_institution'
    id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_consortium = db.Column(db.Boolean)
    consortium_id = db.Column(db.Text)

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()[0:12]
        self.created = datetime.datetime.utcnow().isoformat()
        super(Institution, self).__init__(**kwargs)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)