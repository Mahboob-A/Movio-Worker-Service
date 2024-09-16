import logging
from typing import Callable

import pika

from django.conf import settings

logger = logging.getLogger(__name__)


class CloudAMQPHandler:
    """CloudAMQP Handler Class to Handle RabbitMQ Connections
    """

    def __init__(self) -> None:
        self.broker_url = settings.CLOUD_AMQP_URL
        self.params = pika.URLParameters(self.broker_url)

    def connect(self):
        self.__connection = pika.BlockingConnection(parameters=self.params)
        self.channel = self.__connection.channel()

    def prepare_exchange_and_queue(self) -> None:
        self.channel.exchange_declare(
            exchange=settings.MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_NAME,
            exchange_type=settings.MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_TYPE,
        )

        self.channel.queue_declare(queue=settings.MOVIO_RAW_VIDEO_SUBMISSION_QUEUE_NAME)

        self.channel.queue_bind(
            settings.MOVIO_RAW_VIDEO_SUBMISSION_QUEUE_NAME,
            settings.MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_NAME,
            settings.MOVIO_RAW_VIDEO_SUBMISSION_BINDING_KEY,
        )


class S3VideoConsumerMQ(CloudAMQPHandler):
    """Interface calss to Consume messages from
        Movio API Service [S3 Uploaded Video]
    """

    def consume_messages(self, callback: Callable) -> None:
        try:
            self.connect()
            self.prepare_exchange_and_queue()

            self.channel.basic_consume(
                settings.MOVIO_RAW_VIDEO_SUBMISSION_QUEUE_NAME, callback, auto_ack=True
            )

            logger.info(
                f"\n\n[=> MQ S3 Video Consumer LISTEN]: Message Consumption from Movio API Service [S3 Uploaded Video] - Started."
            )
            self.channel.start_consuming()
            
        except Exception as e:
            logger.exception(
                f"\n\n[XX MQ S3 Video Consumer EXCEPTION]: Exception Occurred During Cnsuming Messages  Movio API Service [S3 Uploaded Video]\n[EXCEPTION]: {str(e)}\n"
            )


s3_video_consumer_mq = S3VideoConsumerMQ()
