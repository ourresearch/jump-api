from sqlalchemy import ForeignKey

from app import db


class GridId(db.Model):
    __tablename__ = 'jump_grid_id'
    institution_id = db.Column(db.Text, ForeignKey('jump_institution.id'), primary_key=True)
    grid_id = db.Column(db.Text, primary_key=True)

    def __repr__(self):
        return "<{} {}, {}>".format(self.__class__.__name__, self.institution_id, self.grid_id)