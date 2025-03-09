# AG News Dataset Processing with PySpark

## Project Overview
This project processes public news data from the AG News dataset using Apache PySpark. It extracts and counts unique words from the 'description' column and stores the processed results in Parquet format. The solution is containerized using Docker and includes automation through GitHub workflows.

## Folder Structure
```
ROOT/
│── .github/
│   ├── workflows/
│   │   ├── docker-image.yml           # GitHub Actions for CI/CD
│── code/
│   ├── config/
│   │   ├── config.yaml                # Configuration file
│   ├── script/
│   │   ├── run.sh                     # Shell script for execution
│   ├── src/
│   │   ├── __pycache__/               # Cached Python files
│   │   ├── __init__.py                # Package initializer
│   │   ├── data_processor.py          # Main data processing script
│   │   ├── logger.py                  # Logging utility
│   │   ├── notebook.ipynb             # Jupyter Notebook for testing
│   │   ├── run.py                     # Main entry point script
│   │   ├── utils.py                   # Helper functions
│   ├── tests/                         # Unit tests
│   ├──Dockerfile                      # Docker configuration
│   ├──github_build_action.yml         # GitHub Actions for CI/CD
│   ├──requirements.txt                # Python dependencies
│── logs/                              # Log files
│── outputs/
│   ├── data/                          # Processed dataset
│   ├── word_count.parquet             # Parquet output
│   ├── word_count_all.parquet         # Processed word count file
│── screenshots/                       # Screenshots of results
│── .gitignore                         # Git ignore file
│── LICENSE                            # License file
│── README.md                          # Project documentation
```

## Features
- Downloads the AG News dataset from Hugging Face.
- Processes the dataset using Apache PySpark.
- Extracts and counts unique words from the 'description' column.
- Saves results in Parquet format.
- Containerized with Docker for deployment.
- Automates workflows using GitHub Actions.

## Setup Instructions

### Prerequisites
- Python 3.11
- PySpark
- Docker
- Jupyter Notebook (optional)

### Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/kasun98/detask.git
   ```
2. Create a virtual environment:
   ```sh
   python -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate
   ```
3. Install dependencies:
   ```sh
   cd code
   pip install -r requirements.txt
   ```
4. Run the data processing script (Both word_count and word_count_all):
   ```sh
   bash ./code/script/run.sh
   ```
5. Run the word_count processing script only:
   ```sh
   python code/src/run.py process_data --cfg code/config/config.yaml --dataset news --dirout outputs
   ```
6. Run the word_count_all processing script only:
   ```sh
   python code/src/run.py process_data_all --cfg code/config/config.yaml --dataset news --dirout outputs
   ```

## Running with Docker
1. You can use the pre-built Docker image from Docker Hub:
   ```sh
   docker run --rm -it --entrypoint /bin/bash dewkd98/dewpublic:latest
   ```
2. Run the data processing pipeline to generate the Parquet files:
   ```sh
   chmod +x /app/code/script/run.sh
   ./code/script/run.sh
   ```
3. Folder structure after execution:
   ```
   code  logs  outputs
   ```
4. View logs:
   ```sh
   cd logs
   cat Data_processed.txt
   cat Data_processed_all.txt
   ```

## Output
The processed dataset is saved in the `outputs/` directory in Parquet format.


## License
This project is licensed under the Apache License.

