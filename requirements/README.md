# Python Requirements

This directory contains the Python dependencies required for the Spark data pipeline project.

## Structure

```
requirements/
├── requirements.txt    # Python package dependencies
└── README.md          # This file
```

## Dependencies Overview

### Core Spark Dependencies

#### `py4j==0.10.9.7`
- **Purpose**: Python for Java bridge, enables Python to interact with Java objects
- **Usage**: Required by PySpark to communicate with Spark's Java/Scala core
- **Version**: Fixed at 0.10.9.7 for compatibility with Spark 3.5.1
- **Critical**: Essential for PySpark functionality

#### `pyspark==3.5.1`
- **Purpose**: Apache Spark Python API for distributed computing
- **Usage**: Core processing engine for ETL jobs and data transformations
- **Version**: Fixed at 3.5.1 for stability and compatibility
- **Features**: 
  - DataFrame API
  - SQL support
  - Streaming capabilities
  - Machine learning libraries

### Built-in Python Modules Used

The project also uses several built-in Python modules that don't require installation:

#### `datetime` (Built-in)
- **Purpose**: Date and time manipulation utilities
- **Usage**: Used in Spark jobs for timestamp operations
- **Features**: Date parsing, formatting, and arithmetic operations
- **Note**: Built-in module, no installation required

#### `uuid` (Built-in)
- **Purpose**: UUID (Universally Unique Identifier) generation
- **Usage**: Built-in Python module for generating unique identifiers
- **Features**: UUID v4 generation for data tracking
- **Note**: Built-in module, no installation required

#### `logging` (Built-in)
- **Purpose**: Logging framework for Python applications
- **Usage**: Used extensively in utils for job logging and monitoring
- **Features**: Configurable logging levels and handlers
- **Note**: Built-in module, no installation required

#### `time` (Built-in)
- **Purpose**: Time-related functions
- **Usage**: Used in utils for execution timing and performance monitoring
- **Features**: Time measurement and sleep functions
- **Note**: Built-in module, no installation required

#### `functools` (Built-in)
- **Purpose**: Higher-order functions and operations on callable objects
- **Usage**: Used for decorators in utils (log_execution)
- **Features**: Decorator utilities and function operations
- **Note**: Built-in module, no installation required

#### `typing` (Built-in)
- **Purpose**: Support for type hints
- **Usage**: Used throughout utils for type annotations
- **Features**: Type hints for better code documentation
- **Note**: Built-in module, no installation required

## Installation

### Docker Environment (Recommended)
```bash
# Dependencies are automatically installed in the Docker container
# via the Dockerfile during image build
docker-compose up -d
```

### Local Development
```bash
# Install dependencies in a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements/requirements.txt
```

### Production Deployment
```bash
# Install in production environment
pip install -r requirements/requirements.txt --no-cache-dir
```

## Version Management

### Version Pinning Strategy
- **Core Dependencies**: Fixed versions for stability
  - `py4j==0.10.9.7` - Critical for Spark compatibility
  - `pyspark==3.5.1` - Core processing engine
- **Built-in Modules**: No version management needed
  - `datetime`, `uuid`, `logging`, `time`, `functools`, `typing`

### Compatibility Matrix
| Component | Version | Compatibility |
|-----------|---------|---------------|
| Python    | 3.8+    | All dependencies |
| Spark     | 3.5.1   | PySpark, Py4J |
| Py4J      | 0.10.9.7| Spark 3.5.1 |

## Usage in Project

### Spark Jobs
```python
# All Spark jobs automatically use these dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import uuid
import logging
import time
from functools import wraps
from typing import Dict, Any
```

### Utils Module
```python
# Utils module uses built-in modules extensively
from utils.spark_utils import setup_logging, log_execution
from utils.config import SPARK_CONFIGS, OUTPUT_PATHS

# Logging with built-in modules
logger = setup_logging(__name__)
logger.info(f"Processing started at {datetime.now()}")

# UUID generation for tracking
tracking_id = str(uuid.uuid4())
```

### Data Processing
```python
# Built-in modules for data processing
from datetime import datetime
current_timestamp = datetime.now()

# UUID for data tracking
import uuid
tracking_id = str(uuid.uuid4())

# Logging for monitoring
import logging
logger = logging.getLogger(__name__)
```

## Troubleshooting

### Common Issues

#### Py4J Version Conflicts
```bash
# If Py4J version conflicts occur
pip uninstall py4j
pip install py4j==0.10.9.7
```

#### PySpark Compatibility
```bash
# Ensure PySpark is compatible with Spark version
pip install pyspark==3.5.1 --force-reinstall
```

#### Built-in Module Issues
```bash
# Built-in modules should always be available
python -c "import datetime, uuid, logging, time, functools, typing; print('All built-in modules available')"
```

### Environment Verification
```bash
# Verify all dependencies are correctly installed
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
python -c "import py4j; print(f'Py4J version: {py4j.__version__}')"

# Verify built-in modules
python -c "import datetime, uuid, logging, time, functools, typing; print('All built-in modules working')"
```

## Security Considerations

### Dependency Scanning
```bash
# Scan for security vulnerabilities
pip install safety
safety check -r requirements/requirements.txt
```

### Version Updates
- **Regular Updates**: Check for security patches monthly
- **Breaking Changes**: Test thoroughly before updating core dependencies
- **Compatibility**: Ensure Py4J and PySpark versions are compatible

## Performance Optimization

### Built-in Module Benefits
- **Memory Efficiency**: Built-in modules are optimized
- **Speed**: No import overhead for built-in modules
- **Reliability**: Built-in modules are stable and well-tested

### Spark Configuration
```python
# Optimize Spark for the installed dependencies
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

## Monitoring and Logging

### Dependency Health
```python
# Monitor dependency versions in logs
import pyspark
import py4j

logger.info(f"PySpark version: {pyspark.__version__}")
logger.info(f"Py4J version: {py4j.__version__}")
```

### Built-in Module Monitoring
```python
# Built-in modules are always available
import datetime
import uuid
import logging
import time
import functools
import typing

logger.info("All built-in modules loaded successfully")
```

### Performance Metrics
- **Memory Usage**: Monitor Spark job memory usage
- **Processing Speed**: Track Spark job performance
- **Error Rates**: Monitor Py4J communication errors
- **Logging**: Monitor application logs for issues
