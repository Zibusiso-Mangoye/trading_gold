FROM python:3.10
WORKDIR /usr/dashboard
COPY . .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt
CMD [ "python", "plot_chart.py" ]