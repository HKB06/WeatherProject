FROM python:3.11-bullseye

LABEL maintainer="WeatherProject Team"
LABEL description="Dashboard for Weather Data Analysis with Spark"

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . .
COPY .env* ./

RUN mkdir -p data/raw data/processed data/metadata logs checkpoints

EXPOSE 5000

CMD ["python", "dashboard/app.py"]

