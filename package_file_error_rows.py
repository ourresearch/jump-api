import shortuuid

from app import db


class PackageFileErrorRow(db.Model):
    __tablename__ = 'jump_file_import_error_rows'
    id = db.Column(db.Text, primary_key=True)  # just to make sqlalchemy happy
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    file = db.Column(db.Text)
    errors = db.Column(db.Text)
    sequence = db.Column(db.Integer)

    def to_dict(self):
        return {
            'id': self.id,
            'package_id': self.package_id,
            'file': self.file,
            'errors': self.errors,
        }

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()
        super(PackageFileErrorRow, self).__init__(**kwargs)
