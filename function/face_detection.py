import requests as req
import boto3
import base64
import os
import json

URL = os.environ['URL']
MQ_URL = os.environ['MQ_URL']
API_KEY = os.environ['API_SECRET_KEY']




def get_face_detection_request(content):
    return {
        "analyze_specs": [{
            "content": content,
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }


def get_object(bucket, name):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    print(f"Getting {name} file from {bucket}")
    response = s3.get_object(
        Bucket=bucket,
        Key=name
    )
    print(f"{name} downloaded")
    return response['Body'].read()


def find_faces(img):
    encoded = base64.b64encode(img).decode('UTF-8')
    auth_header = {'Authorization': f'Api-Key {API_KEY}'}
    body = get_face_detection_request(encoded)
    resp = req.post(URL, json=body, headers=auth_header)
    coordinates = []
    try:
        faces = resp.json()['results'][0]['results'][0]['faceDetection']['faces']
        for face in faces:
            coordinates.append(face['boundingBox']['vertices'])
    except KeyError:
        print(f'Cant find faces info in {resp.json()}')
        return []
    return coordinates


def to_message(object_key, face):
    return {
        'object_key': object_key,
        'face': face
    }


def send_to_queue(object_key, faces):
    session = boto3.session.Session()
    sqs = session.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1',
    )
    messages = [to_message(object_key, face) for face in faces]
    for message in messages:
        body = json.dumps(message)
        print(f'Trying to send {body}')
        sqs.send_message(
            QueueUrl=MQ_URL,
            MessageBody=body,
            MessageDeduplicationId=object_key
        )


def handler(event, context):
    bucket = event['messages'][0]['details']['bucket_id']
    name = event['messages'][0]['details']['object_id']
    img = get_object(bucket, name)
    send_to_queue(name, find_faces(img))