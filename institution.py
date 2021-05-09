import datetime

from cached_property import cached_property
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

    def user_permissions(self, is_consortium=None):
        user_ids = db.session.query(UserInstitutionPermission.user_id).filter(
            UserInstitutionPermission.institution_id == self.id).distinct()
        users = User.query.filter(User.id.in_(user_ids)).all()

        permission_dicts = [u.to_dict_permissions()[self.id] for u in users if (u.email and (not u.email.startswith("team+")))]
        # permission_dicts = [u.to_dict_permissions()[self.id] for u in users]

        if is_consortium is not None:
            permission_dicts = [d for d in permission_dicts if d["is_consortium"]==is_consortium]

        return permission_dicts

    @cached_property
    def packages_sorted(self):
        packages = self.packages
        response = [my_package for my_package in packages if not my_package.is_deleted]
        response.sort(key=lambda k: k.package_name, reverse=False)
        response.sort(key=lambda k: k.is_owned_by_consortium, reverse=False) #minor
        response.sort(key=lambda k: k.publisher, reverse=False)  #main sorting key is last
        return response

    @cached_property
    def is_consortium_member(self):
        for my_package in self.packages:
            if my_package.is_owned_by_consortium:
                return True
        return False

    def to_dict(self):
        return {
            'id': self.id,
            'grid_ids': [g.grid_id for g in self.grid_ids],
            'ror_ids': [r.ror_id for r in self.ror_ids],
            'name': self.display_name,
            'is_demo': self.is_demo_institution,
            'is_consortium': self.is_consortium,
            'is_consortium_member': self.is_consortium_member,
            'user_permissions': self.user_permissions(),
            'institutions': self.user_permissions(is_consortium=False),
            'consortia': self.user_permissions(is_consortium=True),
            'publishers': [p.to_dict_minimal() for p in self.packages_sorted],
        }

    def __init__(self, **kwargs):
        self.id = u'institution-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(Institution, self).__init__(**kwargs)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)