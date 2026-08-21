# FROM python:3.12-slim

# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1
# ENV PYTHONPATH=/app

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY ingestion/stream/seismic/ ./seismic/
# COPY utils/ ./utils/

# CMD ["python", "seismic/streamer.py"]


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY ingestion/stream/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/stream/seismic/ ./seismic/
COPY utils/ ./utils/

CMD ["python", "-m", "seismic.streamer"]