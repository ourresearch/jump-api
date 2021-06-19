import datetime
from app import db


class RawFileUploadObject(db.Model):
    __tablename__ = 'jump_raw_file_upload_object'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    file = db.Column(db.Text, primary_key=True)
    bucket_name = db.Column(db.Text)
    object_name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    num_rows = db.Column(db.Numeric(asdecimal=False))
    error = db.Column(db.Text)
    error_details = db.Column(db.Text)
    to_delete_date = db.Column(db.DateTime)

    def __init__(self, **kwargs):
        self.created = datetime.datetime.utcnow().isoformat()
        super(RawFileUploadObject, self).__init__(**kwargs)
