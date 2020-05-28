from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from app import db


class Permission(db.Model):
    __tablename__ = 'jump_permission'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, unique=True)

    @staticmethod
    def get(name):
        return Permission.query.filter(Permission.name == name).first()

    @staticmethod
    def view():
        return Permission.get('view')

    @staticmethod
    def admin():
        return Permission.get('admin')

    @staticmethod
    def modify():
        return Permission.get('modify')

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.name)


class UserInstitutionPermission(db.Model):
    __tablename__ = 'jump_user_institution_permission'
    user_id = db.Column(db.Integer, ForeignKey('jump_user.id'), primary_key=True)
    institution_id = db.Column(db.Integer, ForeignKey('jump_institution.id'), primary_key=True)
    permission_id = db.Column(db.Integer, ForeignKey('jump_permission.id'), primary_key=True)

    user = relationship('User', uselist=False)
    institution = relationship('Institution', uselist=False)

    permission = relationship(Permission)

    def __repr__(self):
        return u'<{} ({}, {}) {}>'.format(self.__class__.__name__, self.user, self.institution, self.permission.name)
