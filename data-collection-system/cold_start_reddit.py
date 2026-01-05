import logging
from pyfaktory import Client, Consumer, Job, Producer

# import time
# import random
import sys

logger = logging.getLogger("faktory test")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


if __name__ == "__main__":
    subreddit = sys.argv[1]
    print(f"Cold starting posts and comments crawl for subreddit {subreddit}")
    # Default url for a Faktory server running locally
    faktory_server_url = "tcp://:password@localhost:7419"

    with Client(faktory_url=faktory_server_url, role="producer") as client:
        producer = Producer(client=client)
        post_job = Job(
            jobtype="scan_posts", args=(subreddit,[]), queue="scan_posts"
        )
        comment_job = Job(
            jobtype="scan_comments", args=(subreddit,[]), queue="scan_comments"
        )
        producer.push(post_job)
        producer.push(comment_job)
