FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and entrypoint script
COPY . .

RUN chmod +x entrypoint.sh

EXPOSE 8050

ENTRYPOINT ["./entrypoint.sh"]
