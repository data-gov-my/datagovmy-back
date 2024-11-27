import os
from datetime import datetime, timedelta

from jose import jwt
from post_office import mail

from data_gov_my.models import Subscription
from data_gov_my.utils import triggers


class TokenEmail():
    def __init__(self, email, language='en-GB'):
        self.sender = 'OpenDOSM <notif@opendosm.my>'
        self.language = language

        try:
            self.subscriber = Subscription.objects.get(email=email)
        except Subscription.DoesNotExist:
            self.subscriber = Subscription.objects.create(email=email)

        self.token = self.generate_token()

    def get_token(self):
        return self.token

    def generate_token(self):
        validity = datetime.now() + timedelta(minutes=5)  # token valid for 5 mins
        jwt_token = jwt.encode({
            'sub': self.subscriber.email,
            'validity': int(validity.timestamp()),
        }, os.getenv("WORKFLOW_TOKEN"))
        return str(jwt_token)

    def get_subject(self):
        if self.subscriber.language == 'ms-MY':
            return 'Sahkan Emel Anda'
        elif self.subscriber.language == 'en-GB':
            return 'Verify Your Email'
        else:
            triggers.send_telegram(
                f'Invalid language selected: {self.subscriber.language}'
            )
            return 'Verify Your Email'

    def send_email(self):
        mail.send(
            sender=self.sender,
            recipients=[self.subscriber.email],
            subject=self.get_subject(),
            html_message=self.get_email_content(),
            priority='now'
        )

    def get_email_content(self):
        token_message_bm = f'''
<p>Terima kasih kerana menjadi pengguna penerbitan kami!</p>
<p>Sila gunakan token berikut untuk mengesahkan emel anda di OpenDOSM:</p>
<p>{self.token}</p>
<p>Sekian, terima kasih<br>Bot Pengesahan OpenDOSM</p>
<hr> 
<i><p>Soalan Lazim: Mengapa token panjang sangat?</p>
<p>Kebanyakan laman web menghantar OTP 6 digit untuk tujuan pengesahan emel. Namun, ramai yang tidak tahu bahawa pendekatan ini memerlukan sumber tambahan (seperti pangkalan data) untuk menyimpan dan menjejaki setiap OTP.</p>
<p>Oleh itu, kami memilih untuk menggunakan JSON Web Tokens (JWTs). JWT adalah cukup panjang untuk dijadikan serba lengkap dan selamat secara kriptografi, maka ia tidak memerlukan sumber tambahan dari segi perkakasan. Ini menjadikan OpenDOSM lebih pantas dan selamat untuk anda!</p></i>
'''

        token_message_en = f'''
<p>Thank you for subscribing!</p>
<p>Please use the following token to authenticate your email on OpenDOSM:</p>
<p>{self.token}</p>
<p>Warm regards,<br>OpenDOSM Authentication Bot</p>
<hr> 
<i><p>Tech FAQ: Why such a long token?</p>
<p>Most websites send a simple 6-digit OTP for email verification. However, this approach requires extra resources (like a database) to store and track each OTP.</p>
<p>Therefore, we use JSON Web Tokens (JWTs) instead. These tokens are long enough to be self-contained and cryptographically secure, meaning they don’t require extra server resources. This makes OpenDOSM faster and more secure for you!</p></i>
'''
        if self.language == 'ms-MY':
            return token_message_bm
        elif self.language == 'en-GB':
            return token_message_en