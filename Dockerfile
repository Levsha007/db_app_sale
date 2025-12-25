FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p static templates backups exports archives

EXPOSE 3000

CMD ["sh", "-c", "sleep 5 && uvicorn main:app --host 0.0.0.0 --port 3000 --reload"]