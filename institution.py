import datetime

from cached_property import cached_property
import shortuuid
from sqlalchemy.orm import relationship
from collections import OrderedDict

from app import db
from app import get_db_cursor
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

    def user_permissions(self, is_consortium=None):
        user_ids = db.session.query(UserInstitutionPermission.user_id).filter(
            UserInstitutionPermission.institution_id == self.id).distinct()
        users = User.query.filter(User.id.in_(user_ids)).all()

        # permission_dicts = [u.to_dict_permissions()[self.id] for u in users if (u.email and (not u.email.startswith("team+")))]
        permission_dicts = [u.to_dict_permissions()[self.id] for u in users]

        if is_consortium is not None:
            permission_dicts = [d for d in permission_dicts if d["is_consortium"]==is_consortium]

        return permission_dicts


    @cached_property
    def feedback_sets(self):
        response = [my_package for my_package in self.packages_sorted if my_package.is_feedback_package]
        return response


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

    @cached_property
    def is_jisc(self):
        from app import JISC_INSTITUTION_ID
        if self.id == JISC_INSTITUTION_ID:
            return True

        # jisc member institutions
        if "jisc" in self.id:
            return True

        # n8_institution_id
        if self.id == "institution-Tfi2z4svqqkU":
            return True

        # test_institution_id
        if self.id == "institution-WzH2RdcHUPoR":
            return True
        # our testing instiutions
        if "institution-testing" in self.id:
            return True

        return False

    def to_dict(self):
        package_dicts = [p.to_dict_minimal() for p in self.packages_sorted]
        package_dicts += [p.to_dict_minimal_feedback_set() for p in self.packages_sorted if p.is_feedback_package]
        return OrderedDict([
            ("id", self.id),
            ("grid_ids", [g.grid_id for g in self.grid_ids]),
            ("ror_ids", [r.ror_id for r in self.ror_ids]),
            ("name", self.display_name),
            ("is_demo", self.is_demo_institution),
            ("is_consortium", self.is_consortium),
            ("is_consortium_member", self.is_consortium_member),
            ("user_permissions", self.user_permissions()),
            ("institutions", self.user_permissions(is_consortium=False)),
            ("consortia", self.user_permissions(is_consortium=True)),
            ("publishers", package_dicts),
            ("consortial_proposal_sets", [p.to_dict_minimal_feedback_set() for p in self.packages_sorted if p.is_feedback_package]),
            ("is_jisc", self.is_jisc)
        ])

    def __init__(self, **kwargs):
        self.id = u'institution-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(Institution, self).__init__(**kwargs)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)