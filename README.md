# Avito Backend Course

## Overview

Homework assignments for the course Backend development in the Master's program [Machine Learning in a Digital Product](https://www.hse.ru/en/ma/mldp/) (HSE University, Faculty of Computer Science & Avito)

### Setup

```bash
# Clone the repository
git clone https://github.com/tlidzhiev/avito-backend-course.git
cd avito-backend-course

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv --python 3.12.11
source .venv/bin/activate

# Install dependencies via uv
uv sync --all-groups

# Install pre-commit
pre-commit install
```

## Project Structure

```
avito-backend-course/
├── src/
│   ├── api/              # API endpoints
│   │   ├── moderation.py # Moderation endpoint
│   │   └── routers.py    # Router configuration
│   ├── schemas/          # Pydantic models
│   │   └── ad.py         # Ad request/response schemas
│   ├── services/         # Business logic
│   │   └── moderation.py # Moderation service
│   └── main.py           # FastAPI application entry point
├── tests/
│   └── test_moderation.py # Unit tests for moderation endpoint
├── pyproject.toml        # Project configuration and dependencies
└── README.md             # This file
```


## Usage

### Start the development server

```bash
# Activate virtual environment (if not already activated)
source .venv/bin/activate

# Run the FastAPI application
fastapi dev src/main.py
```

The API will be available at `http://localhost:8000`

### API Documentation

Once the server is running, you can access:
- Swagger UI: `http://localhost:8000/docs`

### API Endpoints

#### POST /moderation/predict

Predicts whether an ad should be approved for moderation.

**Request Body:**
```json
{
  "seller_id": 1,
  "is_verified_seller": true,
  "item_id": 100,
  "name": "Product Name",
  "description": "Product Description",
  "category": 1,
  "images_qty": 5
}
```

**Response:**
```json
{
  "is_approved": true
}
```

**Business Logic:**
- Verified sellers (`is_verified_seller: true`) are always approved
- Unverified sellers are approved only if they have at least one image (`images_qty > 0`)

## Testing

### Run all tests

```bash
# Activate virtual environment (if not already activated)
source .venv/bin/activate

# Run tests with pytest
pytest tests/

# Run tests with verbose output
pytest tests/ -v

# Run specific test file
pytest tests/test_moderation.py -v
```
