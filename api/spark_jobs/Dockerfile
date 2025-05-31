FROM bitnami/spark:3.4

USER root
RUN install_packages python3-pip && \
    pip install --no-cache-dir boto3 requests

USER 1001
