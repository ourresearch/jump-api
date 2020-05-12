# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid
from package import Package

from app import db

class Account(db.Model):
    __tablename__ = 'jump_account'

    id = db.Column(db.Text, primary_key=True)
    username = db.Column(db.Text)
    display_name = db.Column(db.Text)
    password_hash = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_consortium = db.Column(db.Boolean)
    consortium_id = db.Column(db.Text)
    packages = db.relationship('Package', lazy='subquery', backref=db.backref("account", lazy="subquery"))

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()[0:8]
        self.created = datetime.datetime.utcnow().isoformat()
        self.is_consortium = False
        self.login_uuid = ""
        super(Account, self).__init__(**kwargs)

    @property
    def is_demo_account(self):
        return self.username.startswith("demo")

    def make_unique_demo_packages(self, login_uuid):
        self.login_uuid = login_uuid

    @property
    def unique_packages(self):
        # if self.is_demo_account:
        #     unique_packages = self.packages
        #     for package in unique_packages:
        #         package.package_id = u"demo-package-{}".format(self.login_uuid)
        #     return unique_packages
        return self.packages

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)


