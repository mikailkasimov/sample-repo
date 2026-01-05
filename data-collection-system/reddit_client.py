import requests
import logging
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://oauth.reddit.com/r"
REDDIT_OAUTH_CLIENT_ID = os.getenv('REDDIT_OAUTH_CLIENT_ID')
REDDIT_OAUTH_CLIENT_SECRET = os.getenv('REDDIT_OAUTH_CLIENT_SECRET')
CLIENT_AUTH = requests.auth.HTTPBasicAuth(REDDIT_OAUTH_CLIENT_ID, REDDIT_OAUTH_CLIENT_SECRET)
POST_DATA = {"grant_type": "client_credentials"}

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

class RedditClient:
    def get_newest_posts(self, subreddit, params):
        params_call = "&".join(params)
        api_call = self.build_request([f"{subreddit}", "new", f".json?{params_call}"])
        return self.execute_request(api_call)

    def get_newest_comments(self, subreddit, params):
        params_call = "&".join(params)
        api_call = self.build_request([f"{subreddit}", "comments", f".json?{params_call}"])
        return self.execute_request(api_call)

    def build_request(self, call_pieces=[]):
        api_call = "/".join([API_BASE_URL] + call_pieces)
        return api_call

    def load_access_token(self):
        if os.path.exists("oauth_access_token.json") and os.path.getsize("oauth_access_token.json") != 0:
            with open("oauth_access_token.json","r") as file:
                return json.load(file)
        return None

    def refresh_access_token(self):      
        try:
            r = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                auth = CLIENT_AUTH,
                data = POST_DATA,
                headers = {"User-Agent": "my_name_deez____deez_what_sir/1.0 by Big-Feeling-1320"}
            )
            r.raise_for_status()
            logger.info("Fetched new OAuth access token")
        except Exception as e:
            logger.error(f"Error fetching new OAuth access token: {e}")

        token = r.json()
        with open("oauth_access_token.json","w") as file:
            json.dump(token,file)
        return token

    def execute_request(self, api_call):
        logger.info(f"api call: {api_call}")

        #get access token if its none
        token = self.load_access_token()
        if token is None:
            token = self.refresh_access_token()
        access_token = token["access_token"]
        r = requests.get(
            api_call,
            headers = {
                "User-Agent": "my_name_deez____deez_what_sir/69 by Big-Feeling-1320",
                "Authorization": f"bearer {access_token}"
            }
        )
        if r.status_code == 401:
            self.refresh_access_token() #write new access token
            r.raise_for_status()

        #else status 200
        data = r.json()
        return data

if __name__ == "__main__":

    client = RedditClient()

    posts = client.get_newest_posts(subreddit="sports", params=["limit=100"])
