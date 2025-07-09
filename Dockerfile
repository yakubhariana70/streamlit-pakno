FROM python:3.13.5-slim-bookworm

# Install build dependencies
# RUN apk add --no-cache gcc g++ libffi-dev musl-dev

# Set working directory
WORKDIR /app

# Copy only required files
COPY app/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source files only (not entire context)
COPY app/ ./app/

# CREATE SECRETS.TOML IN STREAMLIT FOLDER
RUN mkdir -p ./app/.streamlit && \
    echo 'files_loc = "/app/files"' > ./app/.streamlit/secrets.toml

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app/main.py", "--server.port=8501", "--server.address=0.0.0.0"]
