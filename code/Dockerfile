# Use pre-installed Conda image based on Debian
FROM conda/miniconda3:latest

# Set environment variables
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.11 (ensure correct Python version)
RUN conda install python=3.11 -y

# Create a new environment and install required packages
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

# Set the working directory
WORKDIR /code

# Set default command to activate the environment
CMD ["/bin/bash", "-c", "source activate myenv && exec bash"]
