from django.core.management.base import BaseCommand

from core_apps.mq_manager.mq_callback import main


class Command(BaseCommand):
    """Consumes Messages from Movio API Service [S3 Uploaded Video]
    """

    help = "Consumes messages from RabbitMQ - Published by Movio API Service [S3 Uploaded Video]"

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS("Consuming messages from Movio API Service [S3 Uploaded Video Data] ...")
        )
        main()
