FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all app files including entrypoint
COPY . .

# Ensure entrypoint.sh is executable
RUN chmod +x entrypoint.sh

# Expose port for Dash
EXPOSE 8050

# Set entrypoint to run initialization + app
ENTRYPOINT ["./entrypoint.sh"]
