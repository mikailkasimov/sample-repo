import datetime
from reddit_client import RedditClient
import os
import time
from pyfaktory import Client, Consumer, Job, Producer
import logging

# these three lines allow psycopg to insert a dict into
# a jsonb coloumn
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("Reddit client")
logger.propagate = False
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_str, logging.INFO)
logger.setLevel(numeric_level)
if not logger.handlers:
    sh = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    logger.addHandler(sh)


# get db url
DATABASE_URL = os.environ.get("DATABASE_URL")
FAKTORY_URL = os.environ.get("FAKTORY_URL")
client = RedditClient()

def json_to_names(posts):
    """
    We expect posts to be the json response returned from client.get_newest_posts() and client.get_newest_comments()
    """
    logger.info("Entering: json_to_names()")
    try:
        result = set([x['data']['name'] for x in posts['data']['children']])
        logger.info(f"json_to_names(): Extracted {len(result)} names from JSON input")
        logger.info(f"Leaving: json_to_names()")
        return result
    except Exception as e:
        logger.error(f"Error in json_to_names(): {e}")
        raise
    
def save_comment_metadata(comments, names_to_process):
    logger.info("Entering: save_comment_metadata()")
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
    for child in comments['data']['children']:
        x = child['data']
        name = x['name']
        if name in names_to_process:
            try:
                subreddit_id = x['subreddit_id']
                text = x['body']
                author_fullname = x['author_fullname']
                link_id = x['link_id']
                parent_id = x['parent_id']
                created_utc = x['created_utc']
                created_utc = datetime.datetime.fromtimestamp(created_utc)
                data = x
                #...
                q = "INSERT INTO reddit_comments (subreddit_id, name, text, author_fullname, link_id, parent_id, created_utc, data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name,created_utc) DO NOTHING;" 

                cur.execute(q, (subreddit_id, name, text, author_fullname, link_id, parent_id, created_utc, data))
                conn.commit()
                logger.info(f"save_comment_metadata(): Saved comment {name} to 'reddit_comments' schema")
            except Exception as e:
                logger.error(f"Error in save_comment_metadata(): {e}")
                raise
    cur.close()
    conn.close()
    logger.info("Leaving: save_comment_metadata()")

def save_post_metadata(posts, names_to_process):
    logger.info("Entering: save_post_metadata()")
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    for child in posts['data']['children']:
        x = child['data']
        name = x['name']
        if name in names_to_process:
            try:
                subreddit_id = x['subreddit_id']
                title = x['title']                        #post title
                text = x['selftext']                      #post text
                flair = x['link_flair_text']              #post flair
                author_fullname = x['author_fullname']    #author name
                url = x['url']                            #url embedded into post
                media = x['media']                        #embedded video metadata (if any) (json)
                created_utc = x['created_utc']
                created_utc = datetime.datetime.fromtimestamp(created_utc)
                data = x
                #...
                q = "INSERT INTO reddit_posts (subreddit_id, name, title, text, author_fullname, url, media, created_utc, data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (name,created_utc) DO NOTHING;"

                cur.execute(q, (subreddit_id, name, title, text, author_fullname, url, media, created_utc, data))
                conn.commit()
                logger.info(f"save_post_metadata(): Saved post {name} to 'reddit_posts' schema")
            except Exception as e:
                logger.error(f"Error in save_post_metadata(): {e}")
                raise
    cur.close()
    conn.close()
    logger.info("Leaving: save_post_metadata()")

def enqueue_scan_comments(subreddit, old_names=[]):
    logger.info(f"Entering: enqueue_scan_comments(): Subreddit: {subreddit}, Old names: {old_names}")
    old_names = set(old_names)
    try:
        new_posts = client.get_newest_comments(subreddit, ["limit=100"])
        logger.info(f"enqueue_scan_comments(): Fetched new comments for '{subreddit}' subreddit")
    except Exception as e:
        logger.error(f"Error in enqueue_scan_comments(): {e}")
        raise

    new_names = json_to_names(new_posts)
    enqueue_process_comments(subreddit, old_names, after="")

    try:
        with Client(faktory_url=FAKTORY_URL, role="producer") as c:
            run_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
            run_at = run_at.isoformat()[:-7] + "Z"
            producer = Producer(client=c)
            job = Job(
                jobtype="scan_comments",
                args=(subreddit, list(new_names)),
                queue="scan_comments",
                at=str(run_at)
            )
            producer.push(job)
            logger.info(f"enqueue_scan_comments(): Enqueued scan_comments job for subreddit: {subreddit}, at: {run_at}")
            logger.info(f"Leaving: enqueue_scan_comments()")
    except Exception as e:
        logger.error(f"Error in enqueue_scan_comments(): {e}")
        raise

def enqueue_scan_posts(subreddit, old_names=[]):
    logger.info(f"Entering: enqueue_scan_posts(): Subreddit: {subreddit}, Old names: {old_names}")
    old_names = set(old_names)
    try:
        new_posts = client.get_newest_posts(subreddit, ["limit=100"])
        logger.info(f"enqueue_scan_posts(): Fetched new posts for '{subreddit}' subreddit")
    except Exception as e:
        logger.error(f"Error in enqueue_scan_posts(): {e}")
        raise

    new_names = json_to_names(new_posts)
    enqueue_process_posts(subreddit, old_names, after="")

    try:
        with Client(faktory_url=FAKTORY_URL, role="producer") as c:
            run_at = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
            run_at = run_at.isoformat()[:-7] + "Z"
            producer = Producer(client=c)
            job = Job(
                jobtype="scan_posts",
                args=(subreddit, list(new_names)),
                queue="scan_posts",
                at=str(run_at)
            )
            producer.push(job)
            logger.info(f"enqueue_scan_posts(): Enqueued scan_posts job for subreddit: {subreddit}, at: {run_at}")
            logger.info(f"Leaving: enqueue_scan_posts()")
    except Exception as e:
        logger.error(f"Error in enqueue_scan_posts(): {e}")      
        raise

def enqueue_process_comments(subreddit, old_names, after):
    logger.info(f"Entering: enqueue_process_comments(): Subreddit: {subreddit}, after: {after}")
    old_names = set(old_names)

    try:
        new_posts = client.get_newest_comments(subreddit, ["limit=100", f"after={after}"])
        logger.info("enqueue_process_comments(): Fetched next batch of comments")
    except Exception as e:
        logger.error(f"Error in enqueue_process_comments(): Error fetching, {e}")
        raise

    new_names = json_to_names(new_posts)
    names_to_process = new_names-old_names
    logger.info(f"enqueue_process_comments(): Found {len(names_to_process)} new comments to process")
    if names_to_process == new_names and old_names:  #means there are over 100 new posts and old_names is not null
        after = new_posts['data']['after']
        try:
            with Client(faktory_url=FAKTORY_URL, role="producer") as c:
                producer = Producer(client=c)
                job = Job(
                    jobtype="process_comments",
                    args=(subreddit, list(old_names), after),
                    queue="process_comments"
                )
                producer.push(job)
                logger.info(f"enqueue_process_comments(): Enqueued process_comments job for subreddit: {subreddit}, after: {after}")
        except Exception as e:
            logger.error(f"Error in enqueue_process_comments(): Queue Error, {e}")
            raise
    save_comment_metadata(new_posts, names_to_process)
    logger.info(f"Leaving: enqueue_process_comments()")
#note for future; fields BEFORE and AFTER used for pagination
def enqueue_process_posts(subreddit, old_names, after):
    logger.info(f"Entering: enqueue_process_posts(): Subreddit: {subreddit}, after: {after}")    
    old_names = set(old_names)

    try:
        new_posts = client.get_newest_posts(subreddit, ["limit=100", f"after={after}"])
        logger.info("enqueue_process_posts(): Fetched next batch of posts")
    except Exception as e:
        logger.error(f"Error in enqueue_process_posts(): Error fetching, {e}")
        raise

    new_names = json_to_names(new_posts)
    names_to_process = new_names-old_names
    logger.info(f"enqueue_process_posts(): Found {len(names_to_process)} new posts to process")

    if names_to_process == new_names and old_names:  #means there are over 100 new posts and old_names is not null
        after = new_posts['data']['after']
        try:
            with Client(faktory_url=FAKTORY_URL, role="producer") as c:
                producer = Producer(client=c)
                job = Job(
                    jobtype="process_posts",
                    args=(subreddit, list(old_names), after),
                    queue="process_posts"
                )
                producer.push(job)
                logger.info(f"enqueue_process_posts(): Enqueued process_posts job for subreddit: {subreddit}, after: {after}")
        except Exception as e:
            logger.error(f"Error in enqueue_process_comments(): Queue Error, {e}")
            raise
    save_post_metadata(new_posts, names_to_process)
    logger.info(f"Leaving: enqueue_process_posts()")



if __name__ == "__main__":
    logger.info("Starting Faktory consumer...")
    with Client(faktory_url=FAKTORY_URL, role="consumer") as c:
        consumer = Consumer(
            client=c,
            queues=["default", "scan_posts", "process_posts", "scan_comments", "process_comments"],
            concurrency=3,
        )
        consumer.register("scan_posts", enqueue_scan_posts)
        consumer.register("scan_comments", enqueue_scan_comments)
        consumer.register("process_posts", enqueue_process_posts)
        consumer.register("process_comments", enqueue_process_comments)
        consumer.run()
        logger.info("Listening for tasks...")
