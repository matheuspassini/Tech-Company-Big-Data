FROM python:3.11-bullseye AS spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      nano \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/opt/spark"
ENV HADOOP_HOME="/opt/hadoop"
ENV LD_LIBRARY_PATH=""
ENV PYTHONPATH=""

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.1-bin-hadoop3.tgz

RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz -o hadoop-3.4.0.tar.gz \
 && tar xfz hadoop-3.4.0.tar.gz --directory /opt/hadoop --strip-components 1 \
 && rm -rf hadoop-3.4.0.tar.gz

FROM spark-base AS pyspark

RUN pip3 install --upgrade pip
COPY requirements/requirements.txt .
RUN pip3 install -r requirements.txt

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

ENV PATH="$SPARK_HOME/sbin:/opt/spark/bin:${PATH}"
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:${PATH}"
ENV PATH="${PATH}:${JAVA_HOME}/bin"

ENV SPARK_MASTER="spark://master:7077"
ENV SPARK_MASTER_HOST="master"
ENV SPARK_MASTER_PORT="7077"
ENV PYSPARK_PYTHON="python3"
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

ENV LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:${LD_LIBRARY_PATH}"

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

COPY yarn/spark-defaults.conf "$SPARK_HOME/conf/"
COPY yarn/*.xml "$HADOOP_HOME/etc/hadoop/"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 600 ~/.ssh/authorized_keys

COPY ssh/ssh_config ~/.ssh/config

COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

EXPOSE 22

ENTRYPOINT ["./entrypoint.sh"]