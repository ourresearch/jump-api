import datetime

import secrets

from app import db


class ResetRequest(db.Model):
    __tablename__ = 'jump_password_reset_request'
    token = db.Column(db.Text, primary_key=True)
    user_id = db.Column(db.Text, db.ForeignKey("jump_user.id"))
    requested = db.Column(db.DateTime)
    expires = db.Column(db.DateTime)

    user = db.relationship("User", lazy='subquery')

    def __init__(self, **kwargs):
        self.token = secrets.token_urlsafe(32)
        self.requested = datetime.datetime.utcnow()
        self.expires = datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        super(ResetRequest, self).__init__(**kwargs)
