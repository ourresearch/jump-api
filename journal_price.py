from app import db


class JournalPrice(db.Model):
    __tablename__ = 'jump_journal_prices'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    publisher = db.Column(db.Text)
    title = db.Column(db.Text)
    issn_l = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    usa_usd = db.Column(db.Numeric)
    year = db.Column(db.Numeric)

    def to_dict(self):
        return {
            'package_id': self.package_id,
            'publisher': self.publisher,
            'title': self.title,
            'issn_l': self.issn_l,
            'subject': self.subject,
            'usa_usd': self.usa_usd,
            'year': self.year,
        }
