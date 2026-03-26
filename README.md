# Data Pipeline Orchestrator

## Overview

This is an **AI-powered Data Pipeline Orchestrator** that automatically generates and deploys Azure Data Factory (ADF) pipelines based on user-provided CSV files and natural language prompts. The system uses Groq's LLaMA 3.3 70B large language model to intelligently decide the pipeline configuration, including data transformations, containers, datasets, and compute settings.

## Architecture

### Core Components

1. **`main.py`** - Command-line interface for the pipeline orchestrator
2. **`dashboard.py`** - Streamlit-based web UI for visual pipeline management
3. **`groq_brain.py`** - AI brain that uses Groq LLM to decide pipeline configurations
4. **`adf_api.py`** - Azure Data Factory REST API integration
5. **`config.py`** - Configuration file for Azure credentials and API keys

### Technology Stack

- **Language**: Python 3.12
- **AI Model**: Groq LLaMA 3.3 70B (for pipeline decision making)
- **Cloud Platform**: Microsoft Azure
  - Azure Data Factory (ADF)
  - Azure Blob Storage
- **UI Framework**: Streamlit
- **Dependencies**: See `requirements.txt`

## How It Works

### High-Level Workflow

```
User Input (CSV + Prompt)
         │
         ▼
   ┌─────────────┐
   │ CSV Schema  │
   │  Extractor  │
   └─────────────┘
         │
         ▼
   ┌─────────────┐
   │   Groq AI   │ ◄── LLaMA 3.3 70B decides:
   │   Brain     │    • Container names
   └─────────────┘    • Pipeline stages
         │            • Transformations
         │            • Compute settings
         ▼            • Partition strategy
   ┌─────────────────────┐
   │  Pipeline Config    │
   │     (JSON)          │
   └─────────────────────┘
         │
         ▼
   ┌─────────────────────┐
   │   ADF Deployment    │
   │  • Create Containers│
   │  • Upload Data      │
   │  • Create Pipelines │
   │  • Execute & Monitor│
   └─────────────────────┘
```

### Step-by-Step Process

1. **CSV Schema Extraction**
   - Reads the CSV file and extracts:
     - Column names
     - Sample data rows (default: 5)
     - Inferred data types (integer, double, string)
     - File size category (small/medium/large)
     - Approximate row count

2. **AI Decision Making (Groq)**
   - Sends schema + user prompt to Groq LLaMA 3.3 70B
   - LLM decides:
     - Container names (e.g., incoming/bronze/silver)
     - Dataset configurations
     - Pipeline types (copy vs. dataflow)
     - Data transformations using ADF expressions
     - Compute settings (core count, memory-optimized vs. general)
     - Partition strategy
     - Execution order
   - Returns complete JSON configuration

3. **Azure Resource Creation**
   - Creates blob containers in Azure Storage
   - Uploads the CSV to the raw/incoming container
   - Creates Linked Service (ADF → Blob Storage)
   - Creates Datasets for source, intermediate, and sink

4. **Pipeline Creation & Execution**
   - **Copy Pipeline**: Moves data from raw → bronze (stage 1)
   - **Data Flow Pipeline**: Applies transformations bronze → silver (stage 2)
   - Executes pipelines in order
   - Monitors status with polling and retry logic

## File Descriptions

### `py_files/main.py`

Command-line interface with the following functions:

- `read_csv_schema()` - Extracts schema from CSV with type inference
- `preview_config()` - Displays AI's pipeline plan before deployment
- `main()` - Orchestrates the full workflow

**User Flow:**
1. Prompts for CSV file path
2. Prompts for natural language pipeline description
3. Reads and analyzes CSV schema
4. Calls Groq AI to generate pipeline configuration
5. Shows preview of the plan
6. Waits for user confirmation
7. Deploys and executes the pipeline
8. Saves configuration to JSON file

### `py_files/groq_brain.py`

AI integration module:

- `decide_pipeline_config()` - Sends schema to Groq API and returns pipeline configuration
- Uses Groq's LLaMA 3.3 70B model
- Implements prompt engineering for ADF pipeline architecture
- Parses and validates JSON response from LLM

**System Prompt Details:**
The LLM is instructed to:
- Decide container naming conventions based on context
- Choose appropriate pipeline types (copy/dataflow)
- Use valid ADF Data Flow expressions
- Include computed columns like `processed_time = currentTimestamp()`
- Provide reasoning for all decisions

### `py_files/adf_api.py`

Azure Data Factory REST API wrapper with functions for:

**Authentication:**
- `get_access_token()` - OAuth2 client credentials flow

**Blob Storage:**
- `create_blob_container()` - Creates Azure Storage containers
- `purge_container()` - Deletes all blobs from a container
- `upload_csv()` - Uploads CSV to blob storage
- `check_blob_has_rows()` - Verifies data exists in container

**ADF Resources:**
- `create_linked_service()` - Creates ADF Linked Service for Blob Storage
- `create_dataset()` - Creates source, intermediate, and sink datasets
- `create_copy_pipeline()` - Creates Copy Activity pipeline
- `create_dataflow_pipeline()` - Creates Data Flow pipeline with transformations
- `publish_factory()` - Publishes factory (waits for propagation)
- `trigger_pipeline()` - Triggers pipeline execution
- `check_pipeline_status()` - Polls pipeline run status with retry logic

**Data Flow Features:**
- `build_dataflow_script()` - Generates ADF Data Flow script
- `rewrite_column_refs()` - Handles ADF reserved keywords in columns
- `_normalize_filter_expr()` - Converts filter expressions to ADF syntax
- `cleanup_old_dataflows()` - Removes old timestamped dataflow versions

### `py_files/dashboard.py`

Streamlit web application providing:

**UI Components:**
- File uploader for CSV
- Text area for pipeline prompt
- Pipeline plan visualization
- Live execution logs with color-coded output
- Progress indicator
- Output CSV download

**Workflow Stages:**
1. **Input**: Upload CSV + enter prompt
2. **Plan**: Review AI-generated pipeline plan
3. **Running**: Execute pipeline with live logs
4. **Done**: Download transformed output
5. **Failed**: Error display with retry option

**Features:**
- Custom CSS styling (modern, clean design)
- Real-time log streaming via threading
- Automatic output fetching from blob storage
- Session state management

### `py_files/config.py`

Configuration file containing:

```python
# Azure Authentication
AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET

# Azure Resources
AZURE_SUBSCRIPTION_ID
AZURE_RESOURCE_GROUP
AZURE_DATA_FACTORY

# Azure Storage
AZURE_STORAGE_ACCOUNT
AZURE_STORAGE_KEY

# External APIs
GROQ_API_KEY
```

## Pipeline Configuration Structure

The AI generates JSON configurations with this structure:

```json
{
  "containers": {
    "raw": "incoming",
    "stage1": "bronze",
    "stage2": "silver"
  },
  "datasets": [
    { "name": "DS_Raw", "container": "incoming", "filename": "*.csv", "role": "source" },
    { "name": "DS_Bronze", "container": "bronze", "filename": "*.csv", "role": "intermediate" },
    { "name": "DS_Silver", "container": "silver", "filename": "output.csv", "role": "sink" }
  ],
  "pipelines": [
    {
      "name": "Pipeline_Raw_to_Bronze",
      "type": "copy",
      "source_dataset": "DS_Raw",
      "sink_dataset": "DS_Bronze",
      "parallel_copies": 4,
      "diu": 4
    },
    {
      "name": "Pipeline_Bronze_to_Silver",
      "type": "dataflow",
      "source_dataset": "DS_Bronze",
      "sink_dataset": "DS_Silver",
      "transformations": ["processed_time = currentTimestamp()", "name = upper(name)"],
      "partition_count": 10,
      "compute_type": "MemoryOptimized",
      "core_count": 8
    }
  ],
  "execution_order": ["Pipeline_Raw_to_Bronze", "Pipeline_Bronze_to_Silver"],
  "reasoning": "Explanation of pipeline decisions"
}
```

## Supported Transformations

The AI can generate ADF Data Flow expressions including:

- **String**: `upper()`, `lower()`, `trim()`, `substring()`, `concat()`, `regexReplace()`
- **Type Conversion**: `toInteger()`, `toDouble()`, `toString()`, `toDate()`
- **Date/Time**: `currentTimestamp()`, `year()`, `month()`, `dayOfMonth()`
- **Conditional**: `iif()`, `iifNull()`, `coalesce()`
- **Aggregation**: `sum()`, `avg()`, `min()`, `max()`, `count()`

## Usage

### Command-Line Interface

```bash
cd py_files
python main.py
```

Enter:
1. Path to CSV file
2. Natural language prompt (e.g., "Clean nulls, uppercase the name column, filter rows where status = 1")

### Web Dashboard

```bash
streamlit run py_files/dashboard.py
```

Then open `http://localhost:8501` in your browser.

## Example Prompts

- "Convert all text columns to uppercase and save to silver"
- "Filter out rows where amount is null, add a processing timestamp"
- "Remove special characters from the email column, convert dates to ISO format"
- "Aggregate sales by region and calculate total revenue"

## Error Handling

- Network errors: Automatic retry with exponential backoff
- Token expiration: Automatic token refresh
- Pipeline failures: Detailed error messages with activity-level diagnostics
- Blob verification: Pre-flight checks before executing ADF pipelines

## Security Notes

- Credentials are stored in `config.py` (should be moved to environment variables or Azure Key Vault in production)
- No Git integration - resources are deployed directly to live ADF
- Containers are purged before each run to ensure clean state

## Dependencies

See `requirements.txt` for full list. Key dependencies:

- `streamlit` - Web UI framework
- `requests` - HTTP client for Azure APIs
- `azure-storage-blob` - Azure Blob Storage SDK
- `pandas` - Data manipulation (if needed)
- `groq` - Groq API client (optional, using requests directly)
