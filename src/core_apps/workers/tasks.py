import os
import subprocess
import logging


from django.conf import settings
from celery import shared_task

from botocore.exceptions import ClientError

from core_apps.common.s3_utils import get_s3_client

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
    local_video_file_path = os.path.join(
        settings.MOVIO_LOCAL_VIDEO_STORAGE_ROOT,
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
        settings.MOVIO_LOCAL_CC_STORAGE_ROOT,
        video_filename_with_extention.split(".")[0],
    )
    local_cc_file_path = f"{local_cc_file_without_extention}.vtt"

    # BASE_DIR/movio-local-cc-files/video_filename.vtt

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
            Bucket=settings.AWS_MOVIO_S3_RAW_CC_SUBTITLE_BUCKET_NAME,
            Key=cc_s3_file_key,
            ExtraArgs={
                "ContentType": f"text/vtt",
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

    local_video_file_path = preprocessed_data["local_video_file_path"]
    local_mp4_video_file_path = local_video_file_path.split(".")[0] + ".mp4"

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
