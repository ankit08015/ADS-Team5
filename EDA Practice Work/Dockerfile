FROM python:3

COPY requirements.txt /usr/local/bin/requirements.txt
WORKDIR /usr/local/bin
RUN pip install -r requirements.txt
COPY . /usr/local/bin
ADD Predicting_Interest_rate_LendingClub.py .

EXPOSE 3000

CMD ["python","./Predicting_Interest_rate_LendingClub.py"]



