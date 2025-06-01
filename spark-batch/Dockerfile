FROM bitnami/spark:latest
USER root
RUN install_packages rsync curl wget python3-pip
RUN pip3 install clickhouse-driver
RUN mkdir -p /opt/spark-apps/jars
COPY spark_batch.py /opt/spark-apps/spark_batch.py
COPY jars/*.jar /opt/spark-apps/jars/
RUN mkdir -p /tmp/.ivy2/local && chmod -R 777 /tmp/.ivy2
WORKDIR /opt/spark-apps
CMD ["spark-submit", "--jars", "/opt/spark-apps/jars/postgresql-42.6.0.jar,/opt/spark-apps/jars/clickhouse-jdbc-0.4.6.jar", "spark_batch.py"]