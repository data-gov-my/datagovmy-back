from datetime import date

from post_office import mail

from data_gov_my.models import Publication, Subscription


def craft_template_en(publication_id, publication_type_title, description):
    description = description[0].lower() + description[1:]
    if description[-1] == '.':
        description = description[:-1]
    return f"""
The Department of Statistics Malaysia (DOSM) has released the latest data and analysis of the {publication_type_title}. The publication contains {description}.

You may access the publication at this link:
https://open.dosm.gov.my/publications/{publication_id}

If you have any questions about the data, you may write to data@dosm.gov.my with your enquiry.

Warm regards,
OpenDOSM Notification Bot

Note: To stop or amend your OpenDOSM notifications, go to: https://open.dosm.gov.my/publications/subscribe

--------
"""


def craft_template_bm(publication_id, publication_type_title, description):
    description = description[0].lower() + description[1:]
    if description[-1] == '.':
        description = description[:-1]
    return f'''
Jabatan Perangkaan Malaysia (DOSM) telah menerbitkan data dan analisis terkini bagi {publication_type_title}. Penerbitan ini mengandungi {description}.

Anda boleh mengakses penerbitan tersebut di pautan ini:
https://open.dosm.gov.my/publications/{publication_id}

Sekiranya anda ada sebarang pertanyaan mengenai data tersebut, anda boleh menghantar enkuiri kepada data@dosm.gov.my.

Sekian, terima kasih.

Bot Notifikasi OpenDOSM

Nota: Untuk menghentikan atau meminda notifikasi anda daripada OpenDOSM, sila ke: https://open.dosm.gov.my/publications/subscribe
'''

def craft_title(title):
    # Extract the month/year within square brackets
    period = title.split(']')[0].strip('[')
    topic = title.split(']')[1]
    new_title = f"{topic}: {period}"
    return new_title

def send_email_to_subscribers():
    # TODO: get_preferred_language()
    today = date.today()
    publications_today = Publication.objects.filter(
        release_date__year=today.year,
        release_date__month=today.month,
        release_date__day=today.day,
        language='en-GB'  # TODO: Get it dynamically
    )

    for p in publications_today:
        subscribers_email = [
            s.email for s in Subscription.objects.filter(publications__contains=[p.publication_type])
        ]

        for s in subscribers_email:
            mail.send(
                sender='notif@opendosm.my',
                recipients=[s],
                subject=craft_title(p.title),
                message=craft_template_en(p.publication_id, p.title, p.description),
                priority='now'
            )

def create_token_message(jwt_token):
    # TODO: handle language preference
    token_message_en = f"""
<p>Thank you for subscribing!</p>

<p>Please use the following token to authenticate your email on OpenDOSM:</p>

<p>{jwt_token}</p>

<p>Warm regards,<br>OpenDOSM Authentication Bot</p>
<hr>
<i><p>Tech FAQ: Why such a long token?</p>

<p>Most websites send a simple 6-digit OTP for email verification. However, this approach requires extra resources (like a database) to store and track each OTP.</p>

<p>Therefore, we use JSON Web Tokens (JWTs) instead. These tokens are long enough to be self-contained and cryptographically secure, meaning they don’t require extra server resources. This makes OpenDOSM faster and more secure for you!</p></i>
"""
    return token_message_en

class OpenDosmMail:
    def __init__(self):
        pass