# coding: utf-8

from cached_property import cached_property
import datetime

from app import db

class AccountGridId(db.Model):
    __tablename__ = 'jump_account_grid_id'

    account_id = db.Column(db.Text, db.ForeignKey("jump_account.id"), primary_key=True)
    grid_id = db.Column(db.Text, primary_key=True)

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.account_id, self.grid_id)


