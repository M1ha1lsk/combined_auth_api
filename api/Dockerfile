FROM python:3.11

RUN apt-get update && apt-get install -y \
    curl gnupg lsb-release \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean

WORKDIR /app/api

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY / .

RUN chmod +x entrypoint.sh && \
    sed -i 's/\r$//' entrypoint.sh  # Fix line endings if needed

EXPOSE 8001

CMD ["./entrypoint.sh", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]