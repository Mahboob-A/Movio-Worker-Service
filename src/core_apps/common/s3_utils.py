import logging
from functools import lru_cache

from django.conf import settings

import boto3
from botocore import config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def get_s3_client():
    try: 
        return boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_S3_REGION_NAME,
            config=config.Config(
                max_pool_connections=20
            )
        )
    except ClientError as e: 
        logger.error(f"Failed to create S3 Clietn: {str(e)}")
        raise e