FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests==2.32.3 srp==1.0.21 pbkdf2==1.3 pycryptodomex==3.20.0
COPY fetch_token.py /app/fetch_token.py
ENTRYPOINT ["python", "/app/fetch_token.py"]
