import os
import uuid
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
    delete_success: bool = False,
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

    video_filename_with_extention = mq_data["video_filename_with_extention"]
    local_file_path = os.path.join(
        settings.MOVIO_LOCAL_VIDEO_STORAGE_ROOT,
        video_filename_with_extention,
    )

    try:
        s3_client.download_file(
            Bucket=settings.AWS_STORAGE_BUCKET_NAME,
            Key=mq_data["s3_file_key"],
            Filename=local_file_path,
        )
        logger.info(
            f"\n\n[=> Video Download Task SUCCESS]: Video Downloaded Successfully from S3.\nFile Name: {video_filename_with_extention}\nFile Path: {local_file_path}\n"
        )
        return generate_chain_result(
            success=True,
            success_message="video-file-download-success",
            mq_data=mq_data,
            # kwargs
            video_filename_with_extention=video_filename_with_extention,
            local_file_path=local_file_path,
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
            local_file_path=preprocessed_data["local_file_path"],
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
