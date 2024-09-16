from .base import * 
from .base import env # noqa 

SECRET_KEY = env("SECRET_KEY")


DEBUG = True

ALLOWED_HOSTS = ["127.0.0.1"]


CSRF_TRUSTED_ORIGINS = [
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8082",
]

# Auth: 8080, API: 8081, Worker: 8082
CORS_ALLOWED_ORIGINS = [
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8082",
]

# DB for dev (although worker service doesn't need DB)
# DATABASES = {
#     "default": {
#         "ENGINE": "django.db.backends.sqlite3",
#         "NAME": BASE_DIR / "db.sqlite3",
#     }
# }

DATABASES = {"default": env.db("DATABASE_URL")}



CORS_URL_REGEX = r"^/api/.*$"

############################ ADDED SETTINGS ###############################

# ########################## Static and Media Settings

STATIC_URL = "/static/"
STATIC_ROOT = str(BASE_DIR / "staticfiles")

MEDIA_URL = "/media/"
MEDIA_ROOT = str(BASE_DIR / "mediafiles")

# ########################## Admin URL

ADMIN_URL = env("ADMIN_URL")


# ##################### Networking

DJANGO_APP_PORT = env("DJANGO_APP_PORT")


# ######################### CELERY CONFIG

CELERY_BROKER_URL = env("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = CELERY_BROKER_URL


CELERY_RESULT_BACKEND_MAX_RETRIES = 15
CELERY_TASK_SEND_SENT_EVENT = True
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

if USE_TZ:
    CELERY_TIMEZONE = TIME_ZONE

# ######################### File Storage

AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
AWS_STORAGE_BUCKET_NAME = env("AWS_STORAGE_BUCKET_NAME")

AWS_S3_REGION_NAME = env("AWS_S3_REGION_NAME")
AWS_QUERYSTRING_AUTH = False  # False will make data public
AWS_S3_FILE_OVERWRITE = False

AWS_S3_CUSTOM_DOMAIN = f"{AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com"

MOVIO_S3_VIDEO_ROOT = "movio-temp-videos"

# ########################## RabbitMQ Config

CLOUD_AMQP_URL = env("CLOUD_AMQP_URL")

MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_NAME = env(
    "MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_NAME"
)
MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_TYPE = env(
    "MOVIO_RAW_VIDEO_SUBMISSION_EXCHANGE_TYPE"
)
MOVIO_RAW_VIDEO_SUBMISSION_QUEUE_NAME = env("MOVIO_RAW_VIDEO_SUBMISSION_QUEUE_NAME")
MOVIO_RAW_VIDEO_SUBMISSION_BINDING_KEY = env("MOVIO_RAW_VIDEO_SUBMISSION_BINDING_KEY")
MOVIO_RAW_VIDEO_SUBMISSION_ROUTING_KEY = env("MOVIO_RAW_VIDEO_SUBMISSION_ROUTING_KEY")


########################################################
# logging

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(levelname)s %(name)-12s %(asctime)s %(module)s  %(process)d %(thread)d %(message)s "
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
    # uncomment for django database query logs
    # 'loggers': {
    #     'django.db': {
    #         'level': 'DEBUG',
    #         'handlers': ['console'],
    #     }
    # }
}
