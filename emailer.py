import os

import jinja2
import sendgrid
from sendgrid.helpers.mail import HtmlContent
from sendgrid.helpers.mail import Mail
from sendgrid.helpers.mail import To
from sendgrid.helpers.mail import Cc
from sendgrid.helpers.mail import From
from sendgrid.helpers.mail import Subject

from app import logger


def create_email(address, subject, template_name, context):
    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(loader=templateLoader)
    html_template = templateEnv.get_template(template_name + ".html")

    html_to_send = html_template.render(context)
    content = HtmlContent(html_to_send)

    from_email = From("support@unsub.org", "Unsub Team")
    to_email = To(address)

    to_emails = [to_email]
    if ("mmu.ac.uk" in address) or ("unimelb.edu.au" in address) or ("cardiff.ac.uk" in address) or ("southwales.ac.uk" in address) or ("le.ac.uk" in address) or ("leicester.ac.uk" in address):
        to_emails += [Cc("scott@ourresearch.org")]
    email = Mail(from_email=from_email, subject=Subject(subject), to_emails=to_emails, html_content=content)

    logger.info(('sending email "{}" to {}'.format(subject, address)))

    return email


def send(email, for_real=False):
    if for_real:
        sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
        email_get = email.get()
        response = sg.client.mail.send.post(request_body=email_get)
        print("Sent an email")
    else:
        print("Didn't really send")



