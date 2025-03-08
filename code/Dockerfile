# Use Conda pre-installed image (Miniconda3)
FROM continuumio/miniconda3:py311

# Set environment variables
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    build-essential \
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
    git \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop (You can install Hadoop from the official Apache repository or download a binary)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz -P /tmp && \
    tar -xzf /tmp/hadoop-3.3.5.tar.gz -C /opt && \
    rm /tmp/hadoop-3.3.5.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop-3.3.5
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install the required dependencies directly into the base Conda environment
RUN conda install -c conda-forge \
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
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /code

# Set default command to activate the base environment
CMD ["bash"]
