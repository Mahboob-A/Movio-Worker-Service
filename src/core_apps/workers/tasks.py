import os
import json
import subprocess
from lxml import etree

import logging


from django.conf import settings
from celery import shared_task, chain, group, chord  # noqa

from botocore.exceptions import ClientError

from core_apps.common.s3_utils import get_s3_client
from core_apps.mq_manager.to_api_service_producer import (
    video_process_result_publisher_mq,
)

logger = logging.getLogger(__name__)

s3_client = get_s3_client()


""" Example MQ Data: 

{
    'video_id': '637b3737-ecf5-4b7d-b705-9d72b5a9a0f8', 

   's3_file_key': 'movio-temp-videos/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2/mkv/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv',

    's3_file_url': 'https://movio-raw-videos.s3.ap-south-1.amazonaws.com/movio-temp-videos/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2/mkv/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv',

    's3_presigned_url': 'https://movio-raw-videos.s3.ap-south-1.amazonaws.com/movio-temp-videos/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2/mkv/7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv?AWSAccessKeyId=AKIA5ONRXXAKM2CTFCWY&Signature=ZX3ICpRQVWKOv29DEet58GFPNlg%3D&Expires=1726571214',

    'video_filename_with_extention': '7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv',

    'user_data': 
        {
            'user_id': 'c29e7edc-fd8b-4fcc-9aa5-714a85ce75cb', 
            'email': 'iammahboob.a@gmail.com'
        }
}


"""


def generate_chain_result(
    success: bool = True,
    exception: str = None,
    error_message: str = None,
    success_message: str = None,
    mq_data: dict = None,
    # If the delete is unsuccessful, the other chain of tasks should continue, hence, for
    #  delete monitoring, using separte variables
    # delete_success=False only indicates that S3 delete was not success, but continue the other tasks to process the video.
    delete_success: bool = True,
    delete_exception: str = None,
    delete_error_message: str = None,
    delete_success_message: str = None,
    **kwargs: dict,
):
    """Generate the result of the chain"""

    return {
        "success": success,
        "exception": exception,
        "error_message": error_message,
        "success_message": success_message,
        "delete_success": delete_success,
        "delete_exception": delete_exception,
        "delete_error_message": delete_error_message,
        "delete_success_message": delete_success_message,
        "mq_data": mq_data,  # dict of dict
        **kwargs,
    }


@shared_task
def download_video_from_s3(mq_data: dict):
    """Download the User Uploaded video file from S3 Bucket"""

    # video_filename_with_extention: 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv
    video_filename_with_extention = mq_data["video_filename_with_extention"]

    # BASE_DIR / movio-local-video-files / tmp-s3-downloads
    if not os.path.exists(settings.MOVIO_LOCAL_VIDEO_STORAGE_S3_DOWNLOAD_DIR):
        os.makedirs(settings.MOVIO_LOCAL_VIDEO_STORAGE_S3_DOWNLOAD_DIR)

    local_video_file_path = os.path.join(
        settings.MOVIO_LOCAL_VIDEO_STORAGE_S3_DOWNLOAD_DIR,
        video_filename_with_extention,
    )

    try:
        s3_client.download_file(
            Bucket=settings.AWS_STORAGE_BUCKET_NAME,  # movio-api-service uploads the user submitted videos to this bucket
            Key=mq_data["s3_file_key"],
            Filename=local_video_file_path,
        )
        logger.info(
            f"\n\n[=> Video Download Task SUCCESS]: Video Downloaded Successfully from S3.\nFile Name: {video_filename_with_extention}\nFile Path: {local_video_file_path}\n"
        )
        return generate_chain_result(
            success=True,
            success_message="video-file-download-success",
            mq_data=mq_data,
            # kwargs
            video_filename_with_extention=video_filename_with_extention,
            local_video_file_path=local_video_file_path,
        )

    except ClientError as e:
        logger.error(
            f"\n\n[XX Video Download Task ERROR XX]: Video Could Not Be Downloaded from S3.\nException: {str(e)}\n"
        )
        return generate_chain_result(
            success=False,
            exception="ClientError",
            error_message=str(e),
            mq_data=mq_data,
        )

    except Exception as e:
        logger.error(
            f"\n\n[XX Video Download Task ERROR XX]: Video Could Not Be Downloaded from S3.\nGeneral Exception: {str(e)}\n"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=mq_data,
        )


@shared_task
def delete_video_file_from_s3(preprocessed_data: dict):
    """Delete the User Uploaded video file from S3 Bucket"""

    if preprocessed_data["success"] == False:
        return preprocessed_data

    try:
        s3_client.delete_object(
            Bucket=settings.AWS_STORAGE_BUCKET_NAME,
            Key=preprocessed_data.get("mq_data").get("s3_file_key"),  # dict of dict
        )
        logger.info(
            f"\n\n[=> Video Deletion Task SUCCESS]: Video Deleted Successfully from S3.\nFile Name: {preprocessed_data['video_filename_with_extention']}\n"
        )
        return generate_chain_result(
            success=True,
            success_message="video-file-delete-success",
            delete_success=True,
            delete_success_message="video-file-delete-success",
            mq_data=preprocessed_data["mq_data"],
            video_filename_with_extention=preprocessed_data[
                "video_filename_with_extention"
            ],
            local_video_file_path=preprocessed_data["local_video_file_path"],
        )
    except ClientError as e:
        logger.error(
            f"\n\n[XX Video Deletion Task ERROR XX]: Video Could Not Be Deleted from S3.\nException: {str(e)}\n"
        )
        return generate_chain_result(
            success=True,  # set to true so that other tasks continue working, as other tasks will only check " success " key.
            exception="ClientError",
            error_message=str(e),
            # delete_success=False only indicates that S3 delete was not success, but continue the other tasks to process the video.
            delete_success=False,
            delete_error_message=str(e),
            delete_exception="ClientError",
            mq_data=preprocessed_data["mq_data"],
        )

    except Exception as e:
        logger.error(
            f"\n\n[XX Video Deletion Task ERROR XX]: Video Could Not Be Deleted from S3.\nGeneral Exception: {str(e)}\n"
        )
        return generate_chain_result(
            success=True,
            exception="Exception",
            error_message=str(e),
            delete_success=False,
            delete_error_message=str(e),
            delete_exception="Exception",
            mq_data=preprocessed_data["mq_data"],
        )


@shared_task(bind=True, max_retries=3)
def extract_cc_from_video(self, preprocessed_data: dict):
    """Extract Closed Captions from the video file"""

    if preprocessed_data["success"] == False:
        return preprocessed_data

    # video_filename_with_extention: 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv
    video_filename_with_extention = preprocessed_data.get("mq_data").get(
        "video_filename_with_extention"
    )
    local_cc_file_without_extention = os.path.join(
        settings.MOVIO_LOCAL_CC_STORAGE_ROOT,  # BASE_DIR/movio-local-cc-files
        video_filename_with_extention.split(".")[0],
    )
    # BASE_DIR/movio-local-cc-files/video_filename.vtt
    local_cc_file_path = f"{local_cc_file_without_extention}.vtt"

    local_video_file_path = preprocessed_data["local_video_file_path"]

    command = [
        "ffmpeg",
        "-i",
        local_video_file_path,
        "-map",
        "0:s:0",
        "-f",
        "webvtt",
        "-y",
        local_cc_file_path,
    ]

    try:
        subprocess.run(command, check=True)
        logger.info(
            f"\n\n[=> SUBTITLE EXTRACTION SUCCESS]: Subtitle from Video Extraction Success of file: {video_filename_with_extention}\n"
        )

        return generate_chain_result(
            success=True,
            success_message="subtitle-extraction-success",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_cc_file_path=local_cc_file_path,
        )

    except subprocess.CalledProcessError as e:
        logger.error(
            f"\n\n[XX SUBTITLE EXTRACTION ERROR XX]: Subtitle from Video Extraction Failed.\nException: {str(e)}\nRetrying...\n"
        )
        if self.request.retries < self.max_retries:
            retry_in = 2**self.request.retries
            logger.warning(
                f"\n[## SUBTITLE EXTRACTION WARNING]: Ffmpeg Command to Extract Subtile Video Rerying in: {retry_in}\n"
            )
            self.retry(exc=e, countdown=retry_in)

        return generate_chain_result(
            success=False,
            exception="subprocess.CalledProcessError",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )
    except Exception as e:
        logger.error(
            f"\n\n[XX SUBTITLE EXTRACTION ERROR XX]: Subtitle from Video Extraction Failed.\nGeneral Exception: {str(e)}\n"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )


@shared_task(bind=True, max_retries=3)
def upload_subtitle_to_translate_lambda(self, preprocessed_data: dict):
    """Translate the Subtitles

    A Lambda Function is running to batch process the .vtt file translation.

    The Lambda Function is triggered by an S3 Event.

    The lambda function translates the "en" vtt file into: Bengali, Hindi, Frensch, and Spanish.
        - Once translated, the vtt file is delted.

    The translalted vtt files are stored in anotehr S3 bucket where the video segments are stored for easy access.
        - Strucure: s3-bucket-name/subtitles/uuid__name/lang_en.vtt, lang_bn.vtt, lang_hi.vtt, lang_fr.vtt, lang_es.vtt
    """

    if preprocessed_data["success"] == False:
        return preprocessed_data

    local_video_file_path = preprocessed_data["local_video_file_path"]
    local_cc_file_path = preprocessed_data["local_cc_file_path"]
    video_filename_with_extention = preprocessed_data.get("mq_data").get(
        "video_filename_with_extention"
    )

    # video_filename_with_extention: 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv
    cc_s3_file_key = video_filename_with_extention.split(".")[0] + ".vtt"

    try:
        s3_client.upload_file(
            Filename=local_cc_file_path,
            Bucket=settings.AWS_MOVIO_S3_RAW_CC_SUBTITLE_BUCKET_NAME,  # subtitle bucket
            Key=cc_s3_file_key,
            ExtraArgs={
                "ContentType": "text/vtt",
            },
        )
        logger.info(
            f"\n\n[=>  SUBTITLE UPLOAD TO TRANSLATE LAMBDA SUCCESS]: Subtitle Upload to S3 Successful: {cc_s3_file_key}"
        )
        return generate_chain_result(
            success=True,
            success_message="subtitle-upload-to-translate-lambda-success",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_cc_file_path=local_cc_file_path,
        )
    except ClientError as e:
        logger.error(
            f"\n\n[XX SUBTITLE UPLOAD TO TRANSLATE LAMBDA ERROR XX]: Subtitle Could Not Be Uploaded to S3.\nException: {str(e)}\n"
        )
        if self.request.retries < self.max_retries:
            retry_in = 2**self.request.retries
            logger.warning(
                f"\n\n[## SUBTITLE UPLOAD TO TRANSLATE LAMBDA WARNING ]: ClientError: The Local Subtitle {cc_s3_file_key} Couldn't be Uploaded To S3.\nRetrying in: {retry_in}.\n"
            )
            raise self.retry(exc=e, countdown=retry_in)
        else:
            return generate_chain_result(
                success=False,
                exception="ClientError",
                error_message=str(e),
                mq_data=preprocessed_data["mq_data"],
            )
    except Exception as e:
        logger.error(
            f"\n\n[XX SUBTITLE UPLOAD TO TRANSLATE LAMBDA ERROR XX]: Subtitle Could Not Be Uploaded to S3.\nGeneral Exception: {str(e)}\n"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )


@shared_task(bind=True, max_retries=3)
def transcode_video_to_mp4(self, preprocessed_data: dict):
    """Transcode the video into mp4 for dash segmentation.

    As dash player can't play .mkv container files, transcode to mp4 for easy streaming.
    """
    if preprocessed_data["success"] == False:
        return preprocessed_data

    local_video_file_path = preprocessed_data[
        "local_video_file_path"
    ]  # local video file path is the .mkv file path
    local_mp4_video_file_path = (
        local_video_file_path.split(".")[0] + ".mp4"
    )  # discard .mkv and add .mp4

    command = [
        "ffmpeg",
        "-i",
        local_video_file_path,
        "-map",
        "0:v",
        "-map",
        "0:a",
        "-b:v",
        "800k",
        "-s:v",
        "640x360",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        local_mp4_video_file_path,
    ]

    try:
        subprocess.run(command, check=True)
        logger.info(
            f"\n[=> DASH TRANSCODE VIDEO SUCCESS]: Task {transcode_video_to_mp4.name}: FFmpeg command to transcode file - {local_video_file_path} executed successfully"
        )
        return generate_chain_result(
            success=True,
            success_message="transcode-video-to-mp4-success",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=local_mp4_video_file_path,
            local_cc_file_path=preprocessed_data["local_cc_file_path"],
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            f"\n[XX DASH TRANSCODE VIDEO ERROR XX]: Task {transcode_video_to_mp4.name}: FFmpeg command to transcode file - {local_video_file_path}  failed\n[Exception]: {str(e)}"
        )
        if self.request.retries < self.max_retries:
            retry_in = 2**self.request.retries
            logger.warning(
                f"\n[## TRANSCODE VIDEO WARNING]: Ffmpeg Command to Transcode Video Rerying in: {retry_in}.\nError: {str(e)}"
            )
            self.retry(exc=e, countdown=retry_in)
        else:
            return generate_chain_result(
                success=False,
                exception="subprocess.CalledProcessError",
                error_message=str(e),
                mq_data=preprocessed_data["mq_data"],
            )
    except Exception as e:
        logger.warning(
            f"\n[## TRANSCODE VIDEO ERROR]: Ffmpeg Command to Transcode Video Failed\nError: {str(e)}"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )


@shared_task(bind=True, max_retries=3)
def dash_segment_video(self, preprocessed_data: dict):
    """Segment video file using ffmpeg."""

    if preprocessed_data["success"] == False:
        return preprocessed_data

    # video_filename_with_extention: 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2.mkv
    video_filename_with_extention = preprocessed_data.get("mq_data").get(
        "video_filename_with_extention"
    )

    # 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2
    raw_video_filename = video_filename_with_extention.split(".")[0]

    local_mp4_video_file_path = preprocessed_data["local_mp4_video_file_path"]

    # BASE_DIR / movio-local-video-files / tmp-segments / 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2 / manifest.mpd
    mp4_segment_files_output_dir = os.path.join(
        settings.MOVIO_LOCAL_VIDEO_STORAGE_SEGMENTS_ROOT_DIR, raw_video_filename
    )

    # BASE_DIR / movio-local-video-files / tmp-segments / 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2
    os.makedirs(mp4_segment_files_output_dir, exist_ok=True)

    logger.info(
        f"\n\n[=> DASH SEGMENT VIDEO STARTED]: DASH Segmentation Started for Video File: {local_mp4_video_file_path}"
    )

    command = [
        "ffmpeg",
        "-i",
        local_mp4_video_file_path,
        "-filter_complex",
        "[0:v]split=3[v1][v2][v3]; [v1]scale=w=1280:h=720[720p]; [v2]scale=w=854:h=480[480p]; [v3]scale=w=640:h=360[360p]",
        "-map",
        "[720p]",
        "-c:v:0",
        "libx264",
        "-b:v:0",
        "2400k",
        "-map",
        "[480p]",
        "-c:v:1",
        "libx264",
        "-b:v:1",
        "1200k",
        "-map",
        "[360p]",
        "-c:v:2",
        "libx264",
        "-b:v:2",
        "800k",
        "-map",
        "0:a?",
        "-init_seg_name",
        "init-stream$RepresentationID$.m4s",
        "-media_seg_name",
        "chunk-stream$RepresentationID$-$Number%05d$.m4s",
        "-use_template",
        "1",
        "-seg_duration",
        "4",
        "-adaptation_sets",
        "id=0,streams=v id=1,streams=a",
        "-f",
        "dash",
        os.path.join(mp4_segment_files_output_dir, "manifest.mpd"),
    ]

    try:
        subprocess.run(command, check=True)

        logger.info(
            f"\n[=> DASH SEGMENT VIDEO SUCCESS]: Task {dash_segment_video.name}: FFmpeg command executed successfully"
        )
        return generate_chain_result(
            success=True,
            success_message="dash-segment-video-success",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=preprocessed_data["local_video_file_path"],
            local_mp4_video_file_path=local_mp4_video_file_path,
            mp4_segment_files_output_dir=mp4_segment_files_output_dir,
            local_cc_file_path=preprocessed_data["local_cc_file_path"],
        )

    except subprocess.CalledProcessError as e:
        logger.error(
            f"\n[XX DASH SEGMENT VIDEO CalledProcessError ERROR XX]: Task {dash_segment_video.name}: FFmpeg command failed\n[Exception]: {e}"
        )
        return generate_chain_result(
            success=False,
            exception="subprocess.CalledProcessError",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )

    except Exception as e:
        logger.error(
            f"\n[XX DASH SEGMENT VIDEO Exception ERROR XX]: Task {dash_segment_video.name}: FFmpeg command failed\n[Exception]: {e}"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )


@shared_task
def edit_manifest_to_add_subtitle_information(preprocessed_data: dict):
    """Edit manifest file to add subtitle information."""

    if preprocessed_data["success"] == False:
        return preprocessed_data

    mp4_segment_files_output_dir = preprocessed_data["mp4_segment_files_output_dir"]
    manifest_path = os.path.join(mp4_segment_files_output_dir, "manifest.mpd")
    local_video_file_path = preprocessed_data["local_video_file_path"]

    # BASE_DIR / movio-local-video-files / tmp-segments / 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2 / manifest.mpd
    # get uuid_name: 7317dea7-39ac-4311-b6ea-f5920fc90c86__test2 part
    local_video_file_name = os.path.basename(local_video_file_path).split(".")[0]

    def add_subtitle_information(manifest_path):
        """Add subtitle information to manifest file."""

        # See more on the aws lamdcda code for the subtitles strucutere
        tree = etree.parse(manifest_path)
        root = tree.getroot()

        ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}

        period = root.find(".//mpd:Period", namespaces=ns)

        for lang in settings.MOVIO_SUBTITLE_TRANSLATE_TARGET_LANGUAGES:

            adaptation_set = etree.Element(
                "AdaptationSet",
                {
                    "id": str(len(period) + 1),
                    "mimeType": "text/vtt",
                    "lang": lang,
                    "contentType": "text",
                },
            )

            role = etree.SubElement(
                adaptation_set,
                "Role",
                {"schemeIdUri": "urn:mpeg:dash:role:2011", "value": "subtitle"},
            )

            representation = etree.SubElement(
                adaptation_set,
                "Representation",
                {
                    "id": f"subtitle-{lang}",
                    "bandwidth": "256",
                },
            )

            base_url = etree.SubElement(representation, "BaseURL")

            # s3 bucket mpd location: bucket_name/segments/uuid_videoname/manifest.mpd
            #  s3 bucket subtitle location: bucket_name/subtitles/uuid_videoname/lang_en.vtt
            base_url.text = f"../../subtitles/{local_video_file_name}/lang_{lang}.vtt"

            period.append(adaptation_set)

        tree.write(
            manifest_path, pretty_print=True, xml_declaration=True, encoding="utf-8"
        )

    try:
        add_subtitle_information(manifest_path)

        logger.info(
            f"\n[=> EDIT MANIFEST TO ADD SUBTITLE INFORMATION SUCCESS]: Task {edit_manifest_to_add_subtitle_information.name}: Edit and Add Subtitle Information is Success"
        )
        return generate_chain_result(
            success=True,
            success_message="edit-manifest-to-add-subtitle-information-success",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=preprocessed_data["local_mp4_video_file_path"],
            mp4_segment_files_output_dir=mp4_segment_files_output_dir,
            local_cc_file_path=preprocessed_data["local_cc_file_path"],
        )
    except Exception as e:
        logger.error(
            f"\n[XX EDIT MANIFEST TO ADD SUBTITLE INFORMATION ERROR XX]: Task {edit_manifest_to_add_subtitle_information.name}: Edit and Add Subtitle Information Failed\n[Exception]: {e}"
        )
        return generate_chain_result(
            success=False,
            exception="Exception",
            error_message=str(e),
            mq_data=preprocessed_data["mq_data"],
        )


# Sub Task to upload segments as batch upload to S3 (created by: upload_dash_segments_to_s3_and_publish_message_callback task)
@shared_task(bind=True, max_retries=5)
def upload_segment_batch_to_s3_sub_task(self, segment_batch: list):
    """Batch upload of segments in S3."""

    failed_segment_uploads = {}

    total_segments = len(segment_batch)
    uploaded_segments = 0

    for local_single_segment_path, s3_file_path in segment_batch:
        try:
            s3_client.upload_file(
                Filename=local_single_segment_path,
                Bucket=settings.AWS_MOVIO_S3_SEGMENTS_SUBTITLES_BUCKET_NAME,
                Key=s3_file_path,
            )
            uploaded_segments += 1
            upload_progress = (uploaded_segments / total_segments) * 100

            logger.info(
                f"[=> SEGMENT S3 BATCH UPLOAD PROGRESS]: {upload_progress:.2f}% ({uploaded_segments}/{total_segments}).\nUploaded Chunk: {os.path.basename(s3_file_path)}"
            )

        except FileNotFoundError as e:
            failed_segment_uploads[s3_file_path] = str(e)
            logger.exception(
                f"\n[XX SEGMENT S3 BATCH UPLOAD ERROR XX]: The Local Segment File: {local_single_segment_path} was not Found.\nException: {str(e)}"
            )

        except ClientError as e:
            logger.warning(
                f"\n[XX SEGMENT S3 BATCH UPLOAD ERROR XX]: S3 Client Error.\nException: {str(e)}\nRetrying to upload: {local_single_segment_path}"
            )
            if self.request.retries < self.max_retries:
                retry_in = 2**self.request.retries
                logger.warning(
                    f"\n[## SEGMENT S3 BATCH UPLOAD WARNING ]: Chunk {os.path.basename(s3_file_path)} Couldn't be Uploaded.\nRetrying in: {retry_in}."
                )
                raise self.retry(exc=e, countdown=retry_in)

            # only count as failed segment when the max retries is exceeded.
            failed_segment_uploads[s3_file_path] = str(e)

        except Exception as e:
            failed_segment_uploads[s3_file_path] = str(e)
            logger.exception(
                f"\n[XX SEGMENT S3 BATCH UPLOAD ERROR XX]: Unexpected Error Occurred. Segments couldn't be uploaded to S3.\nException: {str(e)}"
            )

    # Result of batch upload.
    if failed_segment_uploads:
        logger.exception(
            f"\n[XX SEGMENT S3 BATCH UPLOAD ERROR XX]: Some Segments Couldn't be Uploaded.\nFailed Segments: {failed_segment_uploads}"
        )
        return "failure"
    else:
        logger.info(
            f"\n[=> SEGMENT S3 BATCH UPLOAD SUCCESS]: Segment Batch UPLOAD SUCCESS."
        )
        return "success"


# Main Entrypoint task to upload segment to S3
@shared_task
def upload_dash_segments_to_s3_and_publish_message_callback(preprocessed_data: dict):
    """
    upload_dash_segments_to_s3_and_publish_message_callback: Main Entrypoint function to upload local single segment file by batch processing in S3 Bucket.

    The task create batches of 10 segments and creates task as group.
    The task uses a chord to upload the segments batches, and a chain of tasks as the callback of the chord.
    The callback chain of tasks includes: publish message to mq and delete local files tasks.
    """

    if preprocessed_data["success"] == False:
        return preprocessed_data

    local_video_file_path = preprocessed_data["local_video_file_path"]
    local_mp4_video_file_path = preprocessed_data["local_mp4_video_file_path"]
    mp4_segment_files_output_dir = preprocessed_data["mp4_segment_files_output_dir"]
    local_cc_file_path = preprocessed_data["local_cc_file_path"]

    try:

        logger.info(
            f"\n\n[=> MAIN DASH SEGMENTS BATCH S3 UPLOAD STARTED]: DASH Segments Batch Creation for S3 Upload Stared for Directroy: {mp4_segment_files_output_dir}"
        )

        # S3 File Structure: segments/uuid__videoname/all-segment-files and mpd file
        local_video_file_name = os.path.basename(local_video_file_path).split(".")[0]

        #  bucket/segments/uuid__videoname/all-segment-files
        s3_main_file_path = (
            f"{settings.AWS_MOVIO_S3_SEGMENTS_BUCKET_ROOT}/{local_video_file_name}/"
        )

        segment_batchs = []
        current_batch = []
        segment_batch_size = 10

        for root, dirs, files in os.walk(mp4_segment_files_output_dir):
            for file in files:
                local_single_segment_path = os.path.join(root, file)
                s3_file_key = os.path.join(s3_main_file_path, file)

                # Tuple[0]: local single segment file path.
                # Tuple[1]: s3 file path for s3 bucket
                current_batch.append((local_single_segment_path, s3_file_key))

                if len(current_batch) >= segment_batch_size:
                    segment_batchs.append(current_batch)
                    current_batch = []

        # If the segment_batch size is < 10
        if current_batch:
            segment_batchs.append(current_batch)

        data = generate_chain_result(
            success=True,
            success_message="DASH Segments Batch Creation for S3 Upload Success.",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=local_mp4_video_file_path,
            mp4_segment_files_output_dir=mp4_segment_files_output_dir,
            local_cc_file_path=local_cc_file_path,
        )

        # using group to upload all the segments parallely
        segment_upload_group = group(
            upload_segment_batch_to_s3_sub_task.s(single_batch)
            for single_batch in segment_batchs
        )

        # Callback chain for the chord: publish mq message, and dlete local files.
        callback_chain = chain(
            publish_video_process_message_mq.s(data),
            local_file_cleanup_callback.s(data),
        )

        # using chord so that cleanup task is only executed when all the upload tasks are completed.
        setment_upload_chord = chord(
            segment_upload_group,
            callback_chain,
        )

        setment_upload_chord.apply_async()

        logger.info(
            f"\n\n[=> MAIN DASH SEGMENTS BATCH S3 UPLOAD COMPLETED]: DASH Segments Batch Creation for S3 Upload Completed."
        )
        return data
    except Exception as e:
        logger.error(
            f"\n[XX MAIN DASH SEGMENTS BATCH S3 UPLOAD ERROR XX]: Unexpected Error Occurred.\nException: {str(e)}"
        )
        return generate_chain_result(
            success=False,
            success_message="DASH Segments Batch Creation for S3 Upload Failed.",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=local_mp4_video_file_path,
            mp4_segment_files_output_dir=mp4_segment_files_output_dir,
            local_cc_file_path=local_cc_file_path,
        )


@shared_task
def publish_video_process_message_mq(results, preprocessed_data: dict):
    """Publish Video Process Message to MQ to be Consumed by Movio-API-Service

    Callback Chain:
        The first task of the callback chain: publish_video_process_message_mq
        The second task of the callback chain:  local_file_cleanup_callback
        Parent Task of Chord: upload_dash_segments_to_s3_and_publish_message_callback
    """

    if preprocessed_data["success"] == False:
        return preprocessed_data

    local_video_file_path = preprocessed_data["local_video_file_path"]
    local_video_file_name = os.path.basename(local_video_file_path).split(".")[0]
    local_cc_file_path = preprocessed_data["local_cc_file_path"]

    if os.path.exists(local_cc_file_path):
        with open(local_cc_file_path, "r") as subtitle_file:
            subtitle_en_vtt_data = subtitle_file.read()
    else:
        subtitle_en_vtt_data = None

    # s3 manifest.mpd file location:
    s3_manifest_file_url = (
        f"https://{settings.AWS_MOVIO_S3_SEGMENTS_SUBTITLES_BUCKET_NAME}.s3.amazonaws.com/"
        f"{settings.AWS_MOVIO_S3_SEGMENTS_BUCKET_ROOT}/{local_video_file_name}/manifest.mpd"
    )

    mq_data_to_publish = {
        "video_id": preprocessed_data.get("mq_data").get("video_id"),
        "user_id": preprocessed_data.get("mq_data").get("user_data").get("user_id"),
        "email": preprocessed_data.get("mq_data").get("user_data").get("email"),
        "s3_manifest_file_url": s3_manifest_file_url,
        "subtitle_en_vtt_data": subtitle_en_vtt_data,
    }

    try:
        # dict to json
        mq_data_to_publish = json.dumps(mq_data_to_publish)

        video_process_result_publisher_mq.publish_data(
            video_process_data=mq_data_to_publish
        )

        logger.info(
            f"\n\n[=> MESSAGE PUBLISH TO MQ SUCCESS]: Video Process Message Published to MQ Success.\n"
        )
        return generate_chain_result(
            success=True,
            success_message="Video Process Message Published to MQ Success.",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=preprocessed_data["local_mp4_video_file_path"],
            mp4_segment_files_output_dir=preprocessed_data[
                "mp4_segment_files_output_dir"
            ],
            local_cc_file_path=local_cc_file_path,
        )
    except Exception as e:
        logger.error(
            f"\n\n[XX MESSAGE PUBLISH TO MQ ERROR XX]: Unexpected Error Occurred: Task: {publish_video_process_message_mq.name}.\nException: {str(e)}"
        )
        return generate_chain_result(
            success=False,
            success_message="Video Process Message Publish to MQ Failed.",
            mq_data=preprocessed_data["mq_data"],
            local_video_file_path=local_video_file_path,
            local_mp4_video_file_path=preprocessed_data["local_mp4_video_file_path"],
            mp4_segment_files_output_dir=preprocessed_data[
                "mp4_segment_files_output_dir"
            ],
            local_cc_file_path=local_cc_file_path,
        )


@shared_task
def local_file_cleanup_callback(results, preprocessed_data):
    """Deletes the Local Files after the S3 Upload, MQ Message Publish is completed.

    Callback Chain:
        The first task of the callback chain: publish_video_process_message_mq
        The second task of the callback chain:  local_file_cleanup_callback
        Parent Task of Chord: upload_dash_segments_to_s3_and_publish_message_callback
    """

    logger.info(
        f"\n\n[=> LOCAL FILE CLEANUP CALLBACK]: Validating Segments Upload Status."
    )

    # WE CAN ADD SENTRY OR OTHER MECHANISSM TO MONITOR THE UPLOADS STATUS HERE FOR FUTURE UPDATEA
    if all(result == "success" for result in results):
        logger.info(
            f"\n[ => LOCAL FILE CLEANUP CALLBACK]: SEGMENTS BATCH S3 UPLOAD SUCCESS: Task - {upload_dash_segments_to_s3_and_publish_message_callback.name} - is successfull.\nCallback: {local_file_cleanup_callback.name}"
        )
    else:
        logger.error(
            f"\n[XX LOCAL FILE CLEANUP CALLBACK XX]: SEGMENTS BATCH S3 UPLOAD ERROR: Task - {upload_dash_segments_to_s3_and_publish_message_callback.name} - Some segments failed to upload.\nCallback: {local_file_cleanup_callback.name}."
        )

    logger.info(
        f"\n\n[=> LOCAL FILE CLEANUP CALLBACK ]: Starting Cleaning Up Local Files.\n\n"
    )

    # Irrespecitive of preprocessind_data["success"] result, delte teh local files
    local_video_file_path = preprocessed_data["local_video_file_path"]
    local_mp4_video_file_path = preprocessed_data["local_mp4_video_file_path"]
    mp4_segment_files_output_dir = preprocessed_data["mp4_segment_files_output_dir"]
    local_cc_file_path = preprocessed_data["local_cc_file_path"]

    local_file_cleanup_success = False
    local_segments_cleanup_success = False
    try:

        if os.path.exists(local_video_file_path):
            os.remove(local_video_file_path)

        if os.path.exists(local_mp4_video_file_path):
            os.remove(local_mp4_video_file_path)

        if os.path.exists(local_cc_file_path):
            os.remove(local_cc_file_path)

        local_file_cleanup_success = True
        logger.info(
            "\n[=>  LOCAL FILE CLEANUP CALLBACK SUCCESS]: Local Files Cleanup Success."
        )
    except (FileNotFoundError, Exception) as e:
        logger.error(
            f"""\n[XX LOCAL FILE CLEANUP CALLBACK ERROR XX]: Error Occurred During Local File Cleanup.\n
            Callback: {local_file_cleanup_callback.name}.\nException: {str(e)}
            """
        )

    try:
        for root, dirs, files in os.walk(mp4_segment_files_output_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(mp4_segment_files_output_dir)

        local_segments_cleanup_success = True
        logger.info(
            "\n[=>  LOCAL FILE CLEANUP CALLBACK SUCCESS]: Segment Files Cleanup Success."
        )

    except (FileNotFoundError, Exception) as e:
        logger.error(
            f"""\n\n[XX SEGMENTS UPLOAD S3 GROUP CALLBACK/CLEANUP ERROR XX]: Error Occurred During Local Segments Cleanup.\n
                 Callback: {local_file_cleanup_callback.name} .\nException: {str(e)}
            """
        )

    return generate_chain_result(
        success=True,
        success_message="Local File Cleanup Success.",
        mq_data=preprocessed_data["mq_data"],
        local_video_file_path=local_video_file_path,
        local_mp4_video_file_path=local_mp4_video_file_path,
        mp4_segment_files_output_dir=mp4_segment_files_output_dir,
        local_cc_file_path=local_cc_file_path,
        local_file_cleanup_success=local_file_cleanup_success,
        local_segments_cleanup_success=local_segments_cleanup_success,
    )


# Ennd Of Tasks.
