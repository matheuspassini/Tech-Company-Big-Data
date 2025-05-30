# Tech Data Lake

This project implements a Data Lake using Apache Spark and Hadoop, configured with Docker for easy development and deployment.

## Architecture

The project consists of the following components:

- **Master Node**: Manages the Spark cluster and coordinates operations
- **History Server**: Provides web interface for job monitoring
- **Worker Nodes**: Execute distributed tasks (scalable as needed)

## Requirements

- Docker
- Docker Compose
- Git

## Project Structure

```
projeto3/
├── data/               # Directory for data storage
├── jobs/              # Spark applications
├── yarn/              # Hadoop/YARN configurations
├── ssh/               # SSH configurations for node communication
├── requirements/      # Python dependencies
├── Dockerfile        # Docker image configuration
├── docker-compose.yml # Service configuration
└── .env.data-lake    # Environment variables
```

## How to Use

1. Clone the repository:
```bash
git clone <repository-url>
cd projeto3
```

2. Start the cluster with the desired number of workers:
```bash
docker-compose -p tech-data-lake -f docker-compose.yml up -d --scale worker=3
```