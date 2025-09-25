# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Vector Database Management System (VDBMS) Differential Fuzzing Framework** written in Python. The framework performs differential testing across four popular vector databases:

- **Milvus**: Port 19530
- **Chroma**: Port 8000  
- **Qdrant**: Port 6333
- **Weaviate**: Port 8080

The project uses raw HTTP APIs instead of SDKs to avoid third-party dependencies and directly test the database interfaces.

## Key Commands

### Running Tests
```bash
# Install dependencies
pip install -r requirements.txt

# Run the main fuzzing framework (default 50 tests)
python main.py

# Test database connections first
python test_connections.py

# Run custom test count (modify main.py:104)
# Change: results = await fuzzer.run_fuzz_test(num_tests=50)
```

### Configuration
```bash
# Configuration is auto-generated from config.json
# Modify config.json to change:
# - Database connection settings (host, port)
# - Test settings (vector_dimension, timeout_seconds, etc.)
# - Collection names for each database

# Default config.json location and structure is automatically created
# if missing, using defaults in config.py:47-77
```

### Dependencies
- `requests==2.31.0` - HTTP requests
- `numpy==1.24.3` - Vector operations
- `pytest==7.4.0` - Testing framework
- `aiohttp==3.8.5` - Async HTTP client
- `asyncio-mqtt==0.13.0` - MQTT support

## Architecture

### Core Components

**Main Framework (`main.py`)**
- `VDBMSFuzzer` class orchestrates the entire testing process
- Handles database setup, test execution, and cleanup
- Gracefully handles partial database failures with mock mode detection
- Default test count: 50 (configurable in main.py:104)
- Entry point: `asyncio.run(main())` at main.py:113

**Database Clients (`db_clients.py`)**
- Abstract `DatabaseClient` base class with common interface
- Specific implementations for each VDBMS:
  - `MilvusClient` - Handles multiple API versions (/v1, /v2)
  - `ChromaClient` - REST API client
  - `QdrantClient` - HTTP API with backward compatibility
  - `WeaviateClient` - GraphQL-based operations
- Each client implements standard operations: insert_vectors, search_vectors, delete_vectors

**Differential Testing (`differential_tester.py`)**
- `DifferentialTester` class executes operations across all databases concurrently
- Compares results using database-specific comparators
- Handles different response formats and edge cases
- Logs inconsistencies when databases behave differently

**Fuzz Generator (`fuzz_generator.py`)**
- `FuzzGenerator` creates randomized test cases
- Supports operations: insert, search, delete, batch_insert, batch_search, mixed_operations
- Generates edge cases: empty vectors, large dimensions, NaN/inf values, malformed IDs
- Configurable probabilities for different types of invalid inputs

### Data Models (`models.py`)

**TestResult Dataclass**:
- `test_id`: Unique test identifier (format: "test_0001")
- `operation`: Type of operation performed
- `inputs`: Input parameters used
- `results`: Database-specific results
- `inconsistencies`: List of found inconsistencies
- `execution_time`: Performance metrics per database (float values)

**DatabaseResult Dataclass** (in differential_tester.py:17):
- `database`: Database name
- `success`: Boolean operation status
- `data`: Response data
- `error`: Optional error message
- `execution_time`: Individual database execution time

### Configuration System

**Configuration (`config.py`, `config.json`)**
- `Config` class manages database connection settings with automatic fallback
- `DatabaseConfig` dataclass for individual database settings
- Default settings: 128-dimensional vectors, 30-second timeout
- Automatic config file creation if missing (defaults in config.py:47-77)
- Configuration validation and auto-fixing in utils.py `ConfigValidator`

### Testing Strategy

**Test Flow:**
1. Setup database connections and create test collections
2. Generate random/edge-case test operations
3. Execute operations concurrently across all databases
4. Compare results and identify inconsistencies
5. Log results and generate reports

**Supported Operations:**
- **Insert**: Single and batch vector insertion with metadata
- **Search**: Vector similarity search with configurable metrics (L2, cosine, inner product)
- **Delete**: Remove vectors by ID
- **Mixed Operations**: Complex sequences of different operations

**Edge Cases Covered:**
- Invalid vector dimensions (empty, very large)
- Special float values (NaN, infinity)
- Malformed IDs and collection names
- Large batch operations
- Mixed metadata types

### Connection Testing (`test_connections.py`)

**Health Check System:**
- Tests multiple API endpoints per database
- Handles different API versions and protocols
- Provides detailed connection status reporting
- Concurrent testing across all databases
- Identifies configuration issues (e.g., disabled REST APIs)

### Result Analysis (`utils.py`)

**ResultAnalyzer** class:
- Saves test results in JSON format with timestamps
- Generates comprehensive reports with:
  - Overall consistency rates
  - Individual database success rates
  - Operation-specific statistics
  - Top inconsistency listings
- Handles result serialization for complex data structures
- Output directory: `results/` (auto-created)

**HealthChecker** and **ConfigValidator**:
- Database health monitoring via HTTP endpoints
- Configuration validation and auto-fixing
- Connection timeout handling and error recovery

## Database-Specific Details

### Milvus
- Uses HTTP REST API (gRPC not used to avoid SDK dependencies)
- Supports multiple API versions (detected automatically)
- Collections require explicit loading after creation
- Index creation skipped for basic operation testing
- Connection endpoints: `/`, `/health`, `/api/v1/health`, `/v1/health`, etc.

### Chroma
- Uses `/api/v1/` endpoints
- Collections auto-created with HNSW space configuration
- Simple REST API with JSON payloads
- Connection endpoints: `/`, `/api/v1/heartbeat`, `/health`, etc.

### Qdrant
- Uses HTTP API with PUT/POST methods
- Supports both new and legacy collection creation formats
- Points-based data model with payloads
- Connection endpoints: `/`, `/health`, `/metrics`, `/collections`

### Weaviate
- Uses GraphQL for search operations
- Classes instead of collections
- Requires explicit class schema definition
- Uses `vectorizer: "none"` for raw vector operations
- Connection endpoints: `/`, `/.well-known/ready`, `/v1/meta`, `/v1/schema`

## Important Patterns

### Error Handling
- Individual database failures don't stop overall testing
- Mock mode detection allows partial testing when databases are unavailable
- Graceful degradation with partial results
- Comprehensive logging with success/failure indicators

### Async Patterns
- All database operations use `async/await`
- Concurrent execution across databases using `asyncio.gather`
- Timeout handling and exception recovery
- Connection pooling with aiohttp.ClientSession

### Response Comparison
- Database-specific response format handling
- ID-based result comparison for search operations
- Flexible inconsistency detection (50% overlap threshold)
- Execution time tracking for performance analysis

### Configuration
- JSON-based configuration with validation
- Automatic fallback to defaults in config.py
- Runtime configuration validation with ConfigValidator

### Testing Architecture
- Differential testing pattern: same operation → multiple databases → compare results
- Fuzzing approach with configurable probability distributions
- Edge case generation: NaN, infinity, empty vectors, malformed data
- Batch and mixed operation testing scenarios

## Development Notes

The framework is designed for **defensive security testing** of vector databases. It focuses on:
- Finding inconsistencies between database implementations
- Testing edge cases and invalid inputs through fuzzing
- Ensuring API stability across different database versions
- Identifying potential security issues through differential analysis

All interactions use raw HTTP APIs to provide clean, reproducible test cases without SDK abstraction layers. The framework supports graceful degradation when some databases are unavailable, making it robust for continuous testing environments.