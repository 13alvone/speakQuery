FROM python:3.11-slim

# Install build tools for C++ extensions
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends build-essential cmake git \ 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/speakquery

# Copy dependency files first for better caching
COPY requirements.txt requirements-dev.txt ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Copy application source
COPY . .

# Build custom C++ components
RUN python build_custom_components.py --rebuild

# Create non-root user for runtime
RUN useradd -m -r speakquery && chown -R speakquery /opt/speakquery
USER speakquery

EXPOSE 5000

ENTRYPOINT ["./run_all.sh"]
