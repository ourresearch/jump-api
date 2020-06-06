import shortuuid

from app import db


class PackageFileWarning(db.Model):
    __tablename__ = 'jump_package_file_import_warning'
    id = db.Column(db.Text, primary_key=True)  # just to make sqlalchemy happy
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    file = db.Column(db.Text)
    row_no = db.Column(db.Numeric)
    column_name = db.Column(db.Text)
    raw_value = db.Column(db.Text)
    label = db.Column(db.Text)
    message = db.Column(db.Text)

    def to_dict(self):
        return {
            'row_no': self.row_no,
            'column_name': self.column_name,
            'raw_value': self.raw_value,
            'label': self.label,
            'message': self.message,
        }

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()
        super(PackageFileWarning, self).__init__(**kwargs)
