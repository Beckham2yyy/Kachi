FROM python:3.12-slim

WORKDIR /app

# copy only the requirements from current folder
COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copy all other files
COPY . .

CMD ["python3", "Kachi.py"]
