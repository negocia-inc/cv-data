FROM python:3.10.2-slim-buster
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src src
COPY aws_credentials /root/.aws/credentials
COPY csv csv
ENTRYPOINT ["python", "src/select_transfer.py", "select_transfer", "--csv_path", "csv/cleansing.csv"]