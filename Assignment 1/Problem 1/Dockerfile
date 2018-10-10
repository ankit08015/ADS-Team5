FROM python:latest

WORKDIR /usr/local/bin

RUN pip install boto
RUN pip install beautifulsoup4
RUN pip 
RUN pip install lxml

ADD Problem1.py .
ADD config.ini .

CMD ["python", "./Problem1.py"]
