# Use Debian-based image
FROM debian:bookworm-slim

# Set environment variables
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    bash \
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
    liblzma-dev \
    git \
    openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install Miniconda (Conda)
RUN curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh && \
    chmod +x miniconda.sh && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

# Set Conda environment path
ENV PATH=/opt/conda/bin:$PATH

# Create a Conda environment with Python 3.11
RUN /opt/conda/bin/conda create -n myenv python=3.11 -y

# Ensure the container always uses Python 3.11 from Conda
ENV PATH="/opt/conda/envs/myenv/bin:$PATH"

# Install additional Python dependencies from requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN /opt/conda/envs/myenv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the code into the container
COPY . /app/code/

# Set working directory
WORKDIR /app

# Set default command to open bash with Conda's Python 3.11
CMD ["/bin/bash"]
