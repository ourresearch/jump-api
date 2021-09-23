import datetime
import string

import secrets
import shortuuid
from sqlalchemy.orm import relationship
from unidecode import unidecode
from werkzeug.security import check_password_hash

from app import db
from permission import UserInstitutionPermission
from util import authenticated_user_id


class User(db.Model):
    __tablename__ = 'jump_user'
    id = db.Column(db.Text, primary_key=True)
    username = db.Column(db.Text, unique=True)
    display_name = db.Column(db.Text)
    password_hash = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_demo_user = db.Column(db.Boolean)
    email = db.Column(db.Text, unique=True)

    def __init__(self, **kwargs):
        self.id = 'user-{}'.format(shortuuid.uuid()[0:12])
        self.created = datetime.datetime.utcnow().isoformat()
        super(User, self).__init__(**kwargs)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.display_name,
            'email': self.email,
            'username': self.username,
            'is_demo': self.is_demo_user,
            'is_password_set': not check_password_hash(self.password_hash, ''),
            'user_permissions': self.permissions_list(),
            'institutions': self.permissions_list(is_consortium=False),
            'consortia': self.permissions_list(is_consortium=True)
        }

    def permissions_list(self, is_consortium=None):
        dicts = self.to_dict_permissions()
        permission_dicts = sorted(
                list(dicts.values()),
                key=lambda x: (x['is_demo_institution'], unidecode(str(x['institution_name'] or ''))))
        if is_consortium is not None:
            permission_dicts = [d for d in permission_dicts if d["is_consortium"]==is_consortium]
        return permission_dicts

    def to_dict_permissions(self):
        dicts = {}
        for my_permission in self.permissions:
            if my_permission.institution_id not in dicts:
                dicts[my_permission.institution_id] = {
                    'institution_id': my_permission.institution_id,
                    'user_id': self.id,
                    'user_email': self.email,
                    'username': self.username,
                    'permissions': [my_permission.permission.name],
                    'institution_name': my_permission.institution.display_name,
                    'is_consortium': my_permission.institution.is_consortium,
                    'user_name': self.display_name,
                    'is_authenticated_user': authenticated_user_id() == self.id,
                    'is_demo_institution': my_permission.institution.is_demo_institution,
                }
            else:
                dicts[my_permission.institution_id]['permissions'].append(my_permission.permission.name)

        return dicts

    def has_permission(self, institution_id, permission):
        return permission.name in self.to_dict_permissions().get(institution_id, {}).get('permissions', [])

    def __repr__(self):
        return "<{} ({}) {}, {}>".format(self.__class__.__name__, self.id, self.email, self.display_name)


def default_password():
    chars = string.ascii_letters + string.digits
    return ''.join([secrets.choice(chars) for x in range(16)])
