FROM python:3.8-slim

WORKDIR /app

COPY app/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY app/ .

CMD ["streamlit", "run", "main.py", "--server.port", "8080", "--server.address", "0.0.0.0"]
