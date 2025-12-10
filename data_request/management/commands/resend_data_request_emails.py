from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import translation
from post_office import mail

from data_request.models import DataRequest, DataRequestAdminEmail
from data_request.serializers import (
    DataRequestSerializer,
)


class Command(BaseCommand):
    help = "Resend DataRequest emails for ticket_id 1..67 inclusive."

    TPL_SUBMITTED = "data_request_submitted"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Do not send emails; only log what would be sent.",
        )

    def handle(self, *args, **options):
        dry_run: bool = options.get("dry_run", False)

        qs = (
            DataRequest.objects.filter(ticket_id__gte=1, ticket_id__lte=67)
            .select_related("agency")
            .prefetch_related("subscription_set")
        )

        cc_list = list(DataRequestAdminEmail.objects.values_list("email", flat=True))

        total_emails = 0
        errors = 0

        self.stdout.write(
            self.style.NOTICE(
                f"Resending initial emails for ticket_id 1..67"
                + (" (dry-run)" if dry_run else "")
            )
        )

        for obj in qs.order_by("ticket_id"):
            try:
                total_emails += self._process_one(obj, cc_list, dry_run)
            except Exception as e:  # pragma: no cover - defensive guard
                errors += 1
                self.stderr.write(self.style.ERROR(f"[{obj.ticket_id}] Error: {e}"))

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Emails {'planned' if dry_run else 'enqueued'}: {total_emails}"
            )
        )
        if errors:
            raise CommandError(f"Completed with {errors} error(s).")

    def _process_one(self, obj: DataRequest, cc_list: list[str], dry_run: bool) -> int:
        sent = 0

        # Resend the original "submitted" email regardless of status
        try:
            sent += self._send_submitted_email(obj, cc_list, dry_run=dry_run)
        except Exception as e:  # pragma: no cover - defensive guard
            self.stderr.write(self.style.ERROR(f"[{obj.ticket_id}] Error sending: {e}"))

        return sent

    def _send_submitted_email(
        self, obj: DataRequest, cc_list: list[str], dry_run: bool = False
    ) -> int:
        """Resend the initial 'submitted' email .
        Uses the first subscriber's language and name to mirror the original email.
        """
        first_sub = obj.subscription_set.first()
        if not first_sub:
            self.stdout.write(
                f"[{obj.ticket_id}] Skipping submitted: no initial subscriber found."
            )
            return 0
        email_lang = first_sub.language  # 'en-GB' or 'ms-MY'
        lang_code = "en" if email_lang == "en-GB" else "ms"
        with translation.override(lang_code):
            context = DataRequestSerializer(obj).data
        context["name"] = first_sub.name
        recipients = [first_sub.email]
        self._log_or_send(
            obj,
            recipients,
            cc_list,
            self.TPL_SUBMITTED,
            language=email_lang,
            context=context,
            dry_run=dry_run,
            to=first_sub.email,
        )
        return 1

    def _log_or_send(
        self,
        obj: DataRequest,
        recipients: list[str],
        cc_list: list[str],
        template: str,
        language: str,
        context: dict,
        dry_run: bool,
        to: str,
    ) -> None:
        prefix = "(dry-run) " if dry_run else ""
        self.stdout.write(
            f"{prefix}[{obj.ticket_id}] send template={template} lang={language} to={to} recipients={recipients} cc={cc_list}"
        )
        if not dry_run:
            mail.send(
                sender=settings.DATA_GOV_MY_FROM_EMAIL,
                recipients=list(recipients),
                cc=list(cc_list),
                template=template,
                language=language,
                context=context,
                backend="datagovmy_ses",
            )
