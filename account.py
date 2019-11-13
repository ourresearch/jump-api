# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
from app import db

class Account(db.Model):
    __tablename__ = 'jump_account'

    id = db.Column(db.Text, primary_key = True)
    username = db.Column(db.Text)
    display_name = db.Column(db.Text)
    password = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_consortium = db.Column(db.Boolean)
    consortium_id = db.Column(db.Text)

    @property
    def active_package(self):
        # TODO FIX
        return None

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.id, self.display_name)


