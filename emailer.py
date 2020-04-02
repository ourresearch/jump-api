import os

import jinja2
import sendgrid
from sendgrid.helpers.mail.mail import Content
from sendgrid.helpers.mail.mail import Email
from sendgrid.helpers.mail.mail import Mail
from sendgrid.helpers.mail.mail import Personalization

from app import logger


def create_email(address, subject, template_name, context):
    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(loader=templateLoader)
    html_template = templateEnv.get_template(template_name + ".html")

    html_to_send = html_template.render(context)
    content = Content("text/html", html_to_send)

    from_email = Email("team@ourresearch.org", "Unpaywall Journals Team")
    to_email = Email(address)

    email = Mail(from_email, subject, to_email, content)
    personalization = Personalization()
    personalization.add_to(to_email)
    email.add_personalization(personalization)

    logger.info((u'sending email "{}" to {}'.format(subject, address)))

    return email


def send(email, for_real=False):
    if for_real:
        sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
        email_get = email.get()
        response = sg.client.mail.send.post(request_body=email_get)
        print u"Sent an email"
    else:
        print u"Didn't really send"



