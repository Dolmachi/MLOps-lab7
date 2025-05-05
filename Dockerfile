FROM python:3.12

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk \
      curl gnupg \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["python", "src/main.py"]