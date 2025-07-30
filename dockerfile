FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Convert to LF and ensure executable
RUN apt-get update && apt-get install -y dos2unix \
    && dos2unix /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh

EXPOSE 8050

ENTRYPOINT ["/app/entrypoint.sh"]
