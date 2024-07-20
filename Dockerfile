#docker build -t my_spark_app .
#docker run -v $(pwd)/Metrics:/app/Metrics my_spark_app



FROM openjdk:11-jre-slim

# Set environment variables
ENV SPARK_VERSION=3.2.0
ENV HADOOP_VERSION=3.2

# Install required packages
RUN apt-get update && \
    apt-get install -y curl tar

# Download and install Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz && \
    tar -xvf /tmp/spark.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# Install Python and pip
RUN apt-get install -y python3 python3-pip

# Install PySpark
RUN pip3 install pyspark

# Copy your application code to the Docker image
COPY . /app

WORKDIR /app

# Set the entry point
ENTRYPOINT ["spark-submit", "my_spark_job.py"]