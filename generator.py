import psycopg2
import os
import random
import uuid
import time
import threading
import concurrent.futures
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime, timedelta

MAX_USERS = 10000
MAX_CATEGORIES = 500
MAX_MESSAGES = 1000000

T_START = datetime(2010, 1, 1, 00, 00, 00)
T_END = T_START + timedelta(days=365 * 10)


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r %2.2f sec' % (method.__name__, te - ts))
        return result

    return timed


def author_uuid_creation():
    author_uuid = [str(uuid.uuid4()) for i in range(MAX_USERS)]
    return author_uuid


def category_uuid_creation():
    category_uuid = [str(uuid.uuid4()) for i in range(MAX_CATEGORIES)]
    return category_uuid


def message_uuid_creation():
    message_uuid = [str(uuid.uuid4()) for i in range(MAX_MESSAGES)]
    return message_uuid


def creating_users(author_uuid):
    for i in range(MAX_USERS):
        yield author_uuid[i], 'User_' + str(i)


def creating_categories(category_uuid):
    for i in range(MAX_CATEGORIES):
        yield category_uuid[i], 'Category_' + str(i), category_uuid[i]


def creating_messages(message_uuid, category_uuid, author_uuid):
    for i in range(MAX_MESSAGES):
        yield message_uuid[i], 'Text_' + str(i), random.choice(category_uuid), T_START + (
                T_END - T_START) * random.random(), random.choice(
            author_uuid)


@timeit
def inserting_users(cur, author_uuid):
    cur.execute("PREPARE insert_users AS INSERT INTO users VALUES($1, $2)")
    statement = creating_users(author_uuid)
    execute_batch(cur, "EXECUTE insert_users (%s, %s)", iter(statement))


@timeit
def inserting_categories(cur, category_uuid):
    cur.execute("PREPARE insert_categories AS INSERT INTO categories VALUES($1, $2, $3)")
    statement = creating_categories(category_uuid)
    execute_batch(cur, "EXECUTE insert_categories (%s, %s, %s)", iter(statement))


@timeit
def inserting_messages(cur, message_uuid, category_uuid, author_uuid):
    cur.execute("PREPARE insert_messages AS INSERT INTO messages VALUES($1, $2, $3, $4, $5)")
    statement = creating_messages(message_uuid, category_uuid, author_uuid)
    execute_batch(cur, "EXECUTE insert_messages (%s, %s, %s, %s, %s)", iter(statement))


if __name__ == "__main__":
    ts = time.time()

    load_dotenv(dotenv_path='config.env')

    conn = psycopg2.connect(
        user=os.getenv("db_user"),
        password=os.getenv("db_pass"),
        database=os.getenv("db_name"),
        host=os.getenv("db_host"),
        port=os.getenv("db_port")
    )
    cur = conn.cursor()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        author_uuid_creator = executor.submit(author_uuid_creation)
        category_uuid_creator = executor.submit(category_uuid_creation)
        message_uuid_creator = executor.submit(message_uuid_creation)
        author_uuid = author_uuid_creator.result()
        category_uuid = category_uuid_creator.result()
        message_uuid = message_uuid_creator.result()

    user_inserter = threading.Thread(target=inserting_users, args=(cur, author_uuid))
    categories_inserter = threading.Thread(target=inserting_categories, args=(cur, category_uuid))
    messages_inserter = threading.Thread(target=inserting_messages,
                                         args=(cur, message_uuid, category_uuid, author_uuid))
    user_inserter.start()
    categories_inserter.start()
    user_inserter.join()
    categories_inserter.join()
    messages_inserter.start()
    messages_inserter.join()

    conn.commit()
    cur.close()
    conn.close()

    te = time.time()

    print('program execution time: %2.2f sec' % float(te - ts))
