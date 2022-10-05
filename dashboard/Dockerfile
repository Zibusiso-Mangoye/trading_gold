FROM python:3.10

WORKDIR /usr/dashboard

COPY requirements.txt ./
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt