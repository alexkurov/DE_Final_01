FROM python:latest
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY main.py /app/main.py
CMD [ "python", "./main.py" ]