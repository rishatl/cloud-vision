FROM python:3.11.1

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY cropp_image/crop.py .

CMD [ "python", "crop.py" ]
