# 150924, Sunday, 01.00 pm 


ARG PYTHON_VERSION=3.10-bullseye

FROM python:${PYTHON_VERSION} AS python 

FROM python AS python-build-stage
ARG BUILD_ENVIRONMENT=dev

RUN apt-get update && apt-get install --no-install-recommends -y \
        build-essential \
        libpq-dev 

COPY ./requirements . 


RUN pip wheel --wheel-dir /usr/src/app/wheel \
        -r ${BUILD_ENVIRONMENT}.txt 

FROM python AS python-run-stage 

RUN addgroup --system movio && adduser --system --ingroup movio movio

ARG BUILD_ENVIRONMENT=dev 

ARG APP_HOME=/home/movio/app 

ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1 
ENV BUILD_ENV=${BUILD_ENVIRONMENT}

WORKDIR ${APP_HOME}

RUN mkdir -p ${APP_HOME}/staticfiles ${APP_HOME}/mediafiles ${APP_HOME}/movio-local-video-files  ${APP_HOME}/movio-local-cc-files  && \
        chown -R movio:movio ${APP_HOME}/staticfiles ${APP_HOME}/mediafiles ${APP_HOME}/movio-local-video-files ${APP_HOME}/movio-local-cc-files && \
        chmod -R 775 ${APP_HOME}/staticfiles ${APP_HOME}/mediafiles ${APP_HOME}/movio-local-video-files ${APP_HOME}/movio-local-cc-files


RUN apt-get update && apt-get install --no-install-recommends -y \
        libpq-dev \
        gettext netcat \
        ffmpeg \ 
        && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
        && rm -rf /var/lib/apt/lists/*

COPY --from=python-build-stage /usr/src/app/wheel /wheels/

RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/* \
        && rm -rf /wheels/ 


COPY --chown=movio:movio ./docker/dev/django/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//g' /entrypoint.sh
RUN chmod +x /entrypoint.sh


COPY --chown=movio:movio ./docker/dev/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start


COPY --chown=movio:movio ./docker/dev/django/celery/worker/start /start-celeryworker
RUN sed -i 's/\r$//g' /start-celeryworker
RUN chmod +x /start-celeryworker


COPY --chown=movio:movio ./docker/dev/django/celery/flower/start /start-flower
RUN sed -i 's/\r$//g' /start-flower
RUN chmod +x /start-flower


COPY --chown=movio:movio . ${APP_HOME} 

RUN chown movio:movio ${APP_HOME} 

# RUN chown -R movio:movio ${APP_HOME}  && \
#         chmod -R 775 ${APP_HOME}

USER movio

ENTRYPOINT ["/entrypoint.sh"]

