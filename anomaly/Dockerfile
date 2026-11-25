FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /anomaly

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 12345

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "12345"]