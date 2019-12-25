import psycopg2
import os
import random
import uuid
import time
import threading
import concurrent.futures
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta

MAX_USERS = 50000
MAX_CATEGORIES = 10000
MAX_MESSAGES = 100000
words = []
names = []
last_names = []
category_uuid = []


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r %2.2f sec' % (method.__name__, te-ts))
        return result
    return timed


def get_random_date():
    start = datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    end = datetime.now()

    delta = end - start
    random_seconds = random.randrange(int(delta.total_seconds()))

    return start + timedelta(seconds=random_seconds)


def get_random_username(names, last_names):
    username = "{} {}".format(random.choice(names), random.choice(last_names))
    return username


def get_random_text(words):
    random_words = []
    for i in range(random.randint(2, 8)):
        random_words.append(random.choice(words))

    message_text = " ".join(random_words)
    return message_text


def get_random_category_name(words):
    random_words = []
    for i in range(random.randint(1, 4)):
        random_words.append(random.choice(words))

    category_name = " ".join(random_words)
    return category_name


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
        yield author_uuid[i], get_random_username(names, last_names)


def creating_categories():
    for i in range(MAX_CATEGORIES):
        category_uuid.append(str(uuid.uuid4()))
        yield category_uuid[i], get_random_category_name(words), random.choice(category_uuid)


def creating_messages(message_uuid, category_uuid, author_uuid):
    for i in range(MAX_MESSAGES):
        yield message_uuid[i], get_random_text(words), random.choice(category_uuid), get_random_date(), random.choice(author_uuid)

@timeit
def inserting_users(cur, author_uuid):
    cur.execute("PREPARE insert_users AS INSERT INTO users VALUES($1, $2)")
    statement = creating_users(author_uuid)
    execute_batch(cur, "EXECUTE insert_users (%s, %s)", iter(statement))

@timeit
def inserting_categories(cur, category_uuid):
    cur.execute("PREPARE insert_categories AS INSERT INTO categories VALUES($1, $2, $3)")
    statement = creating_categories()
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

    with open('words.txt') as f:
        words = [line.replace('\n', '') for line in f.readlines()]

    with open('first-names.txt') as f:
        names = [line.replace('\n', '') for line in f.readlines()]

    with open('last-names.txt') as f:
        last_names = [line.replace('\n', '') for line in f.readlines()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        author_uuid_creator = executor.submit(author_uuid_creation)
        message_uuid_creator = executor.submit(message_uuid_creation)
        author_uuid = author_uuid_creator.result()
        message_uuid = message_uuid_creator.result()

    user_inserter = threading.Thread(target=inserting_users, args=(cur, author_uuid))
    categories_inserter = threading.Thread(target=inserting_categories, args=(cur, category_uuid))
    messages_inserter = threading.Thread(target=inserting_messages, args=(cur, message_uuid, category_uuid, author_uuid))
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

    print('program execution time: %2.2f sec' % float(te-ts))
