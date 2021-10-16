# TODO: MC 2021-10-15 Requires google / cloud-sdk:latest container - need to look this up
FROM python:3.9-slim


# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV PYTHONPATH /app
WORKDIR $APP_HOME
COPY . ./
RUN pwd
RUN ls -lah
RUN pip install -r requirements.txt
RUN python -m unittest discover ./tests

# Scale up the workers / threads to requirements
CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 0 app:app