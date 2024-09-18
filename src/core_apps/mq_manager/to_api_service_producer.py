
import json
import logging

import pika

from django.conf import settings

logger = logging.getLogger(__name__)


class CloudAMQPHandler:
    """CloudAMQP Helper Class to Declare Exchange and Queue
    for Video Process Result
    """

    def __init__(self) -> None:
        self.broker_url = settings.CLOUD_AMQP_URL
        self.params = pika.URLParameters(self.broker_url)

    def connect(self):
        self.__connection = pika.BlockingConnection(parameters=self.params)
        self.channel = self.__connection.channel()

    def prepare_exchange_and_queue(self) -> None:

        self.channel.exchange_declare(
            exchange=settings.MOVIO_PROCESSED_VIDEO_RESULT_SUBMISSION_EXCHANGE_NAME,
            exchange_type=settings.MOVIO_PROCESSED_VIDEO_RESULT_EXCHANGE_TYPE,
        )
        self.channel.queue_declare(
            queue=settings.MOVIO_PROCESSED_VIDEO_RESULT_QUEUE_NAME
        )

        self.channel.queue_bind(
            settings.MOVIO_PROCESSED_VIDEO_RESULT_QUEUE_NAME,
            settings.MOVIO_PROCESSED_VIDEO_RESULT_SUBMISSION_EXCHANGE_NAME,
            settings.MOVIO_PROCESSED_VIDEO_RESULT_BINDING_KEY,
        )


class VideoProcessResultPublisherMQ(CloudAMQPHandler):
    """Interface Class to Publish Video Process Result Events to Movio-API-Service to Update the DB"""

    def publish_data(self, video_process_data: json) -> None:

        try:
            self.connect()
            self.prepare_exchange_and_queue()

            self.channel.basic_publish(
                exchange=settings.MOVIO_PROCESSED_VIDEO_RESULT_SUBMISSION_EXCHANGE_NAME,
                routing_key=settings.MOVIO_PROCESSED_VIDEO_RESULT_ROUTING_KEY,
                body=video_process_data,
            )
            logger.info(
                f"\n\n[=> MQ VIDEO PROCESS RESULT PUBLISH SUCCESS]: MQ Publish Success.\n\n"
            )
            message = "video-process-result-mq-publish-success."
            return True, message
        except Exception as e:
            logger.exception(
                f"\n\n[XX MQ VIDEO PROCESS RESULT PUBLISH ERROR XX]: MQ Publish Unsuccessful.\n[MQ EXCEPTION]: {str(e)}\n\n"
            )
            message = "video-process-result-mq-publish-error"
            return False, message


video_process_result_publisher_mq = VideoProcessResultPublisherMQ()
