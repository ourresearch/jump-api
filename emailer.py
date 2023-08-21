import os

import jinja2
import requests

from app import logger

_mailgun_api_key = os.getenv('MAILGUN_API_KEY')


def send_email(to_address, subject, template_name, template_data, for_real=False):
    template_loader = jinja2.FileSystemLoader(searchpath='templates')
    template_env = jinja2.Environment(loader=template_loader)
    html_template = template_env.get_template(template_name + '.html')
    html = html_template.render(template_data)

    to_emails = [to_address]

    if (
        ("mmu.ac.uk" in to_address)
        or ("unimelb.edu.au" in to_address)
        or ("cardiff.ac.uk" in to_address)
        or ("southwales.ac.uk" in to_address)
        or ("le.ac.uk" in to_address)
        or ("leicester.ac.uk" in to_address)
    ):
        to_emails += ["richard@ourresearch.org"]

    mailgun_url = f"https://api.mailgun.net/v3/unsub.org/messages"

    mailgun_auth = ("api", _mailgun_api_key)

    mailgun_data = {
        "from": "Unsub Team <support@unsub.org>",
        "to": to_emails,
        "subject": subject,
        "html": html
    }

    logger.info(f'sending email "{subject}" to {to_address}')

    if for_real:
        try:
            requests.post(mailgun_url, auth=mailgun_auth, data=mailgun_data)
            logger.info("Sent an email")
        except Exception as e:
            logger.exception(e)
    else:
        logger.info("Didn't really send")
