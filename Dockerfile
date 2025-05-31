FROM python:3.11

RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y docker-ce-cli && \
    apt-get clean

ENV PYTHONPATH="${PYTHONPATH}:/app"
WORKDIR /app

COPY auth_db/requirements.txt ./auth_db_requirements.txt
COPY api/requirements.txt ./api_requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r auth_db_requirements.txt -r api_requirements.txt

COPY auth_db/ ./auth_db/
COPY api/ ./api/

COPY api/entrypoint.sh .
RUN chmod +x entrypoint.sh && \
    sed -i 's/\r$//' entrypoint.sh  # Fix line endings if needed

EXPOSE 8000 8001

CMD ["sh", "-c", "cd auth_db && alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 & cd /app/api && ./entrypoint.sh uvicorn main:app --host 0.0.0.0 --port 8001"]