FROM python:3.12-slim AS base

WORKDIR /app

# System deps for lxml and other C extensions
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml ./
COPY src/ src/

# Install tributary with all optional dependencies
RUN uv pip install --system ".[all]"

# Download nltk data for sentence chunker
RUN python -c "import nltk; nltk.download('punkt_tab', quiet=True)"

ENTRYPOINT ["tributary"]
CMD ["--help"]
