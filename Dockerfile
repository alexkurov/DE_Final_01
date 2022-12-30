FROM python:latest
RUN apt-get update && apt-get -y install cron vim
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
WORKDIR /app
COPY crontab /etc/cron.d/crontab
COPY main.py /app/main.py
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

# run crond as main process of container
CMD ["cron", "-f"]