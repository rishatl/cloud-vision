import io
import json
import os
import uuid
import random

from PIL import Image
from sanic import Sanic
from sanic.response import empty

import boto3
import ydb
import ydb.iam

app = Sanic(__name__)

ydb_driver: ydb.Driver
config: dict

ACCESS_KEY=os.environ['ACCESS_KEY']
SECRET_KEY=os.environ['SECRET_KEY']


@app.after_server_start
async def after_server_start(app, loop):
    print(f"App listening at port {os.environ['PORT']}")
    global config
    config = {
        'PHOTO_BUCKET': os.environ['PHOTOS_BUCKET'],
        'FACE_BUCKET': os.environ['FACES_BUCKET'],
        'DB_ENDPOINT': os.environ["DB_ENDPOINT"],
        'DB_DATABASE': os.environ["DB_DATABASE"]
    }
    global ydb_driver
    print('start')
    ydb_driver = initDb()
    ydb_driver.wait(timeout=5)


@app.post("/")
async def hello(request):
    print(f"Received request: {request.json}")
    messages = request.json['messages']
    for message in messages:
        try:
            handle(message)
        except Exception as e:
            print(e)
    print("success")
    return empty(status=200)

@app.after_server_stop
async def shutdown():
    print('shutdown')
    ydb_driver.close()

def insertPhotoToDb(original_id, face_id):
    rand = random.Random()
    query = f"""
    INSERT INTO photo (id, original_id, face_id)
    VALUES ({rand.getrandbits(64)}, '{original_id}', '{face_id}');
    """
    print(f"Trying execute query: {query}")
    session = ydb_driver.table_client.session().create()
    session.transaction().execute(query, commit_tx=True)
    session.closing()

def getPhoto(bucket, key):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url='https://storage.yandexcloud.net'
    )
    
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )
  
    return response['Body'].read()

def putPhoto(bucket, key, content):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url='https://storage.yandexcloud.net'
    )
  
    s3.put_object(
        Body=content,
        Bucket=bucket,
        Key=key,
        ContentType='application/octet-stream'
    )

def initDb():
    endpoint = config["DB_ENDPOINT"]
    database = config["DB_DATABASE"]
    creds = ydb.iam.MetadataUrlCredentials()
    driver_config = ydb.DriverConfig(
        endpoint,database,credentials=creds
    )
    return ydb.Driver(driver_config)

def handle(message):
    body = json.loads(message['details']['message']['body'])
    photo = Image.open(io.BytesIO(getPhoto(config['PHOTO_BUCKET'], body['object_key'])))
    face = body['face']
  
    count = 0
    x = set()
    y = set()
  
    for coordinate in face:
        x.add(int(coordinate['x']))
        y.add(int(coordinate['y']))
    
    sorted_x = sorted(x)
    sorted_y = sorted(y)
  
    left = sorted_x[0]
    right = sorted_x[1]
    top = sorted_y[0]
    bottom = sorted_y[1]
  
    face_id = f"{body['object_key'].removesuffix('.jpg')}_{count}.jpg"
  
    cutFace = photo.crop((left, top, right, bottom))
    bytes = io.BytesIO()
    cutFace.save(bytes, format='JPEG')
    putPhoto(config['FACE_BUCKET'], face_id, bytes.getvalue())
    count += 1
  
    insertPhotoToDb(body['object_key'], face_id)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ['PORT']), motd=False, access_log=False)