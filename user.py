import datetime

import shortuuid
from sqlalchemy.orm import relationship

from app import db
from permission import UserInstitutionPermission


class User(db.Model):
    __tablename__ = 'jump_user'
    id = db.Column(db.Text, primary_key=True)
    username = db.Column(db.Text)
    display_name = db.Column(db.Text)
    password_hash = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_demo_user = db.Column(db.Boolean)

    permissions = relationship(UserInstitutionPermission)

    def __init__(self, **kwargs):
        self.id = u'user-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(User, self).__init__(**kwargs)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.display_name,
            'email': self.username,
            'user_permissions': self.permissions_list()
        }

    def permissions_list(self):
        dicts = self.permissions_dict()
        return dicts.values()

    def permissions_dict(self):
        dicts = {}
        for permission in self.permissions:
            if permission.institution_id not in dicts:
                dicts[permission.institution_id] = {
                    'institution_id': permission.institution_id,
                    'user_id': self.id,
                    'user_email': self.username,
                    'permissions': [permission.permission.name]
                }
            else:
                dicts[permission.institution_id]['permissions'].append(permission.permission.name)

        return dicts

    def has_permission(self, institution_id, permission_name):
        return permission_name in self.permissions_dict().get(institution_id, {}).get('permissions', [])

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)