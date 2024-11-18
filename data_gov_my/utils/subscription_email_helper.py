import os

from post_office import mail

from data_gov_my.models import Subscription, Publication
from data_gov_my.utils import triggers
from data_gov_my.utils.publication_helpers import craft_title


def get_email_text(description, publication_id):
    return f"""
{description}

You may access the publication at this link:
https://open.dosm.gov.my/publications/{publication_id}

If you have any questions about the data, you may write to data@dosm.gov.my with your enquiry.

Warm regards,
OpenDOSM Notification Bot

<i>Note: To stop or amend your OpenDOSM notifications, go to: https://open.dosm.gov.my/publications/manage-subscription</i>

-------
"""

class SubscriptionEmail():
    def __init__(self, subscriber, publication_id):
        self.sender = 'OpenDOSM <notif@opendosm.my>'
        self.subscriber = subscriber
        self.subscriber_language = self.subscriber.language
        self.publication = Publication.objects.get(publication_id=publication_id, language=self.subscriber_language)

#     if
#     subscription.language == 'ms-MY':
#     pub = pub_object_bm
#
# elif subscription.language == 'en-GB':
# pub = pub_object_en
# else:
# pub = pub_object_en
# triggers.send_telegram(
#     f'Can\'t determine subscriber\'s {subscription.email} locale. Will use en-GB.'
# )
# mail.send(
#     sender='OpenDOSM <notif@opendosm.my>',
#     recipients=[subscription.email],
#     subject=craft_title(pub.title),
#     message=craft_template_en(
#         pub.publication_id,
#         pub.publication_type_title,
#         pub.description
#     ),
#     priority='now'
# )
    def send_email(self):
        mail.send(
            sender=self.sender,
            recipients=[self.subscriber.email],
            subject=self.get_email_subject(),
            html_message=self.get_email_content(),
            priority='now'
        )

    def get_email_subject(self):
        return craft_title(self.publication.title)

    def get_email_content(self):
        if self.publication.description_email:
            description_email = f'<p>{self.publication.description_email}</p>'
        else:
            description_email = ''
        content_en = f'''
        {description_email}
        <p>The publication is live at this link:</p>
        <p>https://open.dosm.gov.my/publications/{self.publication.publication_id}</p>
        <p>If you have any questions about the data, you may write to data@dosm.gov.my with your enquiry.</p>
        <p>Warm regards,</p>
        <p>OpenDOSM Notification Bot</p>
        <i><p>Note: To stop or amend your OpenDOSM notifications, go to: https://open.dosm.gov.my/publications/manage-subscription</p></i>
'''
        content_bm = f'''
        {description_email}
        <p>Penerbitan tersebut boleh diakses melalui pautan ini:</p>
        <p>https://open.dosm.gov.my/ms-MY/publications/{self.publication.publication_id}</p>
        <p>Sekiranya anda ada sebarang pertanyaan mengenai data tersebut, anda boleh menghantar enkuiri kepada data@dosm.gov.my.</p>
        <p>Sekian, terima kasih.</p>
        <p>Bot Notifikasi OpenDOSM</p>
        <i><p>Nota: Untuk menghentikan atau meminda notifikasi anda daripada OpenDOSM, sila ke: https://open.dosm.gov.my/ms-MY/publications/manage-subscription</p></i> 

'''
        if self.subscriber.language == 'en-GB':
            return content_en
        elif self.subscriber.language == 'ms-MY':
            return content_bm
        else:
            triggers.send_telegram(
                f'Can\'t determine subscriber\'s {self.subscriber.email} locale. Will use en-GB.'
            )
            return content_en

if __name__ == '__main__':
    sub = Subscription.objects.get(email=os.getenv('DJANGO_SUPERUSER_EMAIL'))
    pub = Publication.objects.filter(language=sub.language).get()
    SubscriptionEmail(pub, sub).send_email()
