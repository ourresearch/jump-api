from sqlalchemy import ForeignKey

from app import db


class RorId(db.Model):
    __tablename__ = 'jump_ror_id'
    institution_id = db.Column(db.Text, ForeignKey('jump_institution.id'), primary_key=True)
    ror_id = db.Column(db.Text, primary_key=True)

    def __repr__(self):
        return u"<{} {}, {}>".format(self.__class__.__name__, self.institution_id, self.ror_id)