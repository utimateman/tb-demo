#!/bin/bash

# setup docker
sudo apt-get update
sudo apt-get remove docker docker-engine docker.io
yes | sudo apt install docker.io
sudo systemctl start docker
sudo systemctl enable docker

# setup python environment
yes | apt install python3-pip	

# setup dremio
docker run -d -p 9047:9047 -p 31010:31010 -p 45678:45678 -p 32010:32010 --name demo-dremio dremio/dremio-oss

# setup minio
docker run \
   -d \
   -p 9000:9000 \
   -p 9090:9090 \
   --name demo-minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=minio" \
   -e "MINIO_ROOT_PASSWORD=minio123" \
   quay.io/minio/minio server /data --console-address ":9090"



# Install Spark
yes | apt-get install wget
yes | apt-get install vim

apt install curl mlocate default-jdk   -y

wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz
tar xfz spark-3.1.3-bin-hadoop3.2.tgz -C /usr/local/
ln -sT spark-3.1.3-bin-hadoop3.2 spark
ln -sT spark-3.1.3-bin-hadoop3.2 /usr/local/spark

echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# Add Extra .jar Files
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar 
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.860/aws-java-sdk-bundle-1.11.860.jar 
mv hadoop-aws-3.2.0.jar /usr/local/spark/jars/hadoop-aws-3.2.0.jar 
mv aws-java-sdk-bundle-1.11.860.jar /usr/local/spark/jars/aws-java-sdk-bundle-1.11.860.jar