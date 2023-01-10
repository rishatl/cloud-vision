import asyncio
import json
import logging
import os

from aiogram import Bot, Dispatcher, types
import ydb
import ydb.iam


# Logger initialization and logging level setting
log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO').upper())

# Handlers
async def start(message: types.Message):
    await message.reply('Hello, {}!'.format(message.from_user.first_name))

TOKEN = os.getenv("BOT_TOKEN")
BOT  = Bot(TOKEN)
dp = Dispatcher(BOT)
driver: ydb.Driver
PHOTO_LINK_TEMPLATE = os.getenv("PHOTO_LINK_TEMPLATE")
OBJECT_LINK_TEMPLATE = os.getenv("OBJECT_LINK_TEMPLATE")


def get_driver():
    endpoint = os.getenv("DB_ENDPOINT")
    path = os.getenv("DB_PATH")
    creds = ydb.iam.MetadataUrlCredentials()
    driver_config = ydb.DriverConfig(
        endpoint, path, credentials=creds
    )
    return ydb.Driver(driver_config)





def add_name_to_last_photo(name):
    query = f"""
        SELECT * FROM photo WHERE name is NULL LIMIT 1;
        """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    face_id = ''
    for row in result_sets[0].rows:
        face_id = row.face_id
    if face_id == '':
        return
    query = f"""
    UPDATE photo SET name = '{name}' WHERE face_id = '{face_id}';
    """
    session.transaction().execute(query, commit_tx=True)
    session.closing()


async def find(message: types.Message):
    
    query = f"""
    SELECT DISTINCT original_id, name FROM photo WHERE name = '{name}';
    """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    session.closing()
    if len(result_sets[0].rows) == 0:
        await message.answer(f'No photos with {name}')
    for row in result_sets[0].rows:
        object_id = row.original_id
        photo_url = OBJECT_LINK_TEMPLATE.format(object_id)
        await message.answer_photo(photo=photo_url)



def set_up():
    global driver
    driver = get_driver()
    driver.wait(timeout=5)
    

async def echo(message: types.Message):
    await message.answer(message.text)

async def get_face(message: types.Message):
    query = f"""
    SELECT * FROM photo WHERE name is NULL LIMIT 1;
    """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    session.closing()
    for row in result_sets[0].rows:
        face_id = row.face_id
        photo_url = PHOTO_LINK_TEMPLATE.format(face_id)
        await message.answer_photo(photo=photo_url)
        return


# Functions for Yandex.Cloud
async def register_handlers(dp: Dispatcher):
    """Registration all handlers before processing update."""

    dp.register_message_handler(start, commands=['start'])
    dp.register_message_handler(get_face, commands=['getface'])

    log.debug('Handlers are registered.')


async def process_event(event, dp: Dispatcher):
    """
    Converting an Yandex.Cloud functions event to an update and
    handling tha update.
    """

    update = json.loads(event['body'])
    log.debug('Update: ' + str(update))

    Bot.set_current(dp.bot)
    update = types.Update.to_object(update)
    await dp.process_update(update)

async def handler(event, context):
    """Yandex.Cloud functions handler."""

    if event['httpMethod'] == 'POST':
        BOT  = Bot(TOKEN)
        dp = Dispatcher(BOT)
        set_up()

        await register_handlers(dp)
        await process_event(event, dp)

        return {'statusCode': 200, 'body': 'ok'}
    return {'statusCode': 405}