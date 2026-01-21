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
