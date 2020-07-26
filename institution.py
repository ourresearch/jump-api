import datetime

import shortuuid
from sqlalchemy.orm import relationship

from app import db
from grid_id import GridId
from ror_id import RorId
from permission import UserInstitutionPermission
from user import User


class Institution(db.Model):
    __tablename__ = 'jump_institution'
    id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_consortium = db.Column(db.Boolean)
    consortium_id = db.Column(db.Text)
    old_username = db.Column(db.Text)
    is_demo_institution = db.Column(db.Boolean)

    grid_ids = relationship(GridId, lazy='subquery')
    ror_ids = relationship(RorId, lazy='subquery')
    packages = relationship('Package', lazy='subquery')

    def user_permissions(self):
        user_ids = db.session.query(UserInstitutionPermission.user_id).filter(
            UserInstitutionPermission.institution_id == self.id).distinct()
        users = User.query.filter(User.id.in_(user_ids)).all()
        return [u.permissions_dict()[self.id] for u in users]

    def to_dict(self):
        return {
            'id': self.id,
            'grid_ids': [g.grid_id for g in self.grid_ids],
            'ror_ids': [r.ror_id for r in self.ror_ids],
            'name': self.display_name,
            'is_demo': self.is_demo_institution,
            'is_consortium': self.is_consortium,
            'user_permissions': self.user_permissions(),
            'publishers': [p.to_dict_micro() for p in self.packages],
        }

    def __init__(self, **kwargs):
        self.id = u'institution-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(Institution, self).__init__(**kwargs)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)