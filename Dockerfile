FROM python:3.11-slim

WORKDIR /app
ENV PYTHONBUFFERED 1
# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всех файлов
COPY . .

# Установка прав на .env
RUN chmod 644 .env