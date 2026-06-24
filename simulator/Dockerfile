FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY telecom_log_simulator.py .
COPY billing_generator.py .

# GENERATOR_MODE selects which script to run: 'telecom' (default) or 'billing'
CMD ["sh", "-c", "python ${GENERATOR_SCRIPT:-telecom_log_simulator.py}"]
