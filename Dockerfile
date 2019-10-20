FROM python:3.7.4

WORKDIR /app

COPY pip.txt ./
RUN pip install --no-cache-dir -r pip.txt
COPY . .
EXPOSE 8080
CMD [ "python", "-u", "naive.py" ]