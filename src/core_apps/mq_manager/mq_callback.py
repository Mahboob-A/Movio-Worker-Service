import logging
import json
import traceback

from django.conf import settings

from core_apps.workers.tasks import (
    download_video_from_s3,
    delete_video_file_from_s3,
    extract_cc_from_video,
    upload_subtitle_to_translate_lambda,
    transcode_video_to_mp4,
    dash_segment_video,
    edit_manifest_to_add_subtitle_information, 
    upload_dash_segments_to_s3,
)

from celery import chain, group, chord

from core_apps.mq_manager.from_api_service_consumer import (
    s3_video_consumer_mq,
)


logger = logging.getLogger(__name__)


def callback(channel, method, properties, body):
    """Callback to consume messages from Movio API Service"""

    try:
        # body in bytes, decode to str then dict
        mq_data = json.loads(body.decode("utf-8"))

        celery_pipeline_to_process_video = chain(
            download_video_from_s3.s(mq_data),
            delete_video_file_from_s3.s(),
            extract_cc_from_video.s(),
            upload_subtitle_to_translate_lambda.s(),
            transcode_video_to_mp4.s(),
            dash_segment_video.s(),
            edit_manifest_to_add_subtitle_information.s(), 
            upload_dash_segments_to_s3.s(),
        )

        celery_pipeline_to_process_video.apply_async()

        logger.info(
            f"\n\n[=> MQ Consume Started]: MQ Message Consume Success.\n"
        )

    except Exception as e:
        logger.error(
            f"\n\n[XX MQ Consume Failed XX]: MQ Message Consume Failed.\n"
            f"Error: {str(e)}\n"
            f"Traceback: {traceback.format_exc()}\n"
        )

def main():
    # consuming the messaages from the queue where the Movio API Service publishes the video files data
    
    s3_video_consumer_mq.consume_messages(callback=callback)
