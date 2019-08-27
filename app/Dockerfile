FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

# Install glpk (glpsol):
RUN apt-get update && \
    apt-get install -y --no-install-recommends glpk-utils

# Update paths (for glpk not sure if needed):
ENV LD_LIBRARY_PATH /usr/local/lib:${LD_LIBRARY_PATH}

COPY ./app/requirements.txt /app
RUN pip install --no-cache-dir -r /app/requirements.txt
