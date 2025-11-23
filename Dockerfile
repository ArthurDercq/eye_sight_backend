FROM python:3.12-slim

WORKDIR /eye_sight

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Pas de reload en prod
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--workers", "4"]
