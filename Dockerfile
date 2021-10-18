FROM google/cloud-sdk:latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV PYTHONPATH /app
WORKDIR $APP_HOME
COPY . ./
RUN pwd
RUN ls -lah
RUN apt-get -y update
RUN apt-get -y install git
RUN pip install -r requirements.txt
RUN python -m unittest discover ./tests

# Scale up the workers / threads to requirements
CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 0 app:app