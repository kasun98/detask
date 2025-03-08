# Use Debian-based image
FROM debian:bullseye-slim

# Set environment variables
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    build-essential \
    python3.11 \
    python3.11-distutils \
    libssl-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    liblzma-dev \
    tk-dev \
    libffi-dev \
    liblzma-dev \
    git \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop (You can install Hadoop from the official Apache repository or download a binary)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz -P /tmp && \
    tar -xzf /tmp/hadoop-3.3.4.tar.gz -C /opt && \
    rm /tmp/hadoop-3.3.4.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop-3.3.4
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install Miniconda (Conda)
RUN curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-py311_23.1.0-1-Linux-x86_64.sh -o miniconda.sh && \
    bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh && \
    /opt/conda/bin/conda init bash

# Set Conda environment path
ENV PATH=/opt/conda/bin:$PATH

# Create the environment and install dependencies
RUN conda create -n myenv python=3.11 && \
    conda activate myenv && \
    conda install -c conda-forge \
    pyspark \
    pytorch \
    numpy \
    pandas \
    scipy \
    scikit-learn \
    polars \
    orjson \
    pyarrow \
    awswrangler \
    accelerate \
    duckdb \
    neo4j \
    s3fs \
    umap-learn \
    smart-open \
    onnxruntime \
    spacy \
    sqlalchemy \
    pytest \
    transformers \
    seqeval \
    gensim \
    numba && \
    conda clean --all -f -y

# Install additional Python dependencies from requirements.txt
COPY code/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /code

# Set default command to activate the environment
CMD ["bash"]
