import json
import logging
import requests
from base64 import b64decode
from queue import Queue, Empty
from urllib3.util.retry import Retry
from typing import Dict, List, Tuple
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from concurrent.futures import ThreadPoolExecutor, Future

DEFAULT_USERNAME = "user1"
DEFAULT_PASSWORD = "pass1"

NUMBER_OF_THREADS = 20
QUEUE_TIMEOUT = 30

LOG_LEVEL = logging.INFO

FILENAME_TEMPLATE = "bookface_network_"

NUMBER_OF_RETRIES = 6
OK_STATUS_CODE = 200
ERROR_STATUS_CODES = (400, 401, 403, 404, 500, 502, 503, 504)
BACKOFF_FACTOR = 0.1

HOST = "http://35.188.78.78:8888"
BASE_API_URL = f"{HOST}/api"
LOGIN_API_URL = f"{BASE_API_URL}/login"
USER_API_URL = f"{BASE_API_URL}/user"
FOLLOWERS_BASE_API_CALL = "followers?skip="

USER_INFO_STRING_FIRST_NAME_SUFFIX = b"\x12"
USER_INFO_STRING_LAST_NAME_SUFFIX = b"\x1a"
USER_INFO_STRING_AGE_SUFFIX = b"("

AUTH_HEADER_NAME = "Set-Cookie"
DEFAULT_REQUEST_HEADERS = {
    "Host": "35.188.78.78:8888",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
}


class BookfaceClient:
    """
    Bookface API Client - Wintego Home Task

    This client can be used to scrape a Bookface network, according to the task's instructions.
    Upon creation, the client will automatically try to authenticate using the provided credentials, followed by
    automatically populating the information for the authenticated user.

    Any further actions, such as scraping the Bookface network of the authenticted user, should be called explicitly.

    Args:
        username (str): A valid Bookface username.
        password (str): A valid Bookface password for the provided username.

    Attributes:
        session (requests.Session): Human readable string describing the exception.
        authenticated (bool): Whether or not the client's session is authenticated.
        pool (ThreadPoolExecutor): Used to control the multithreaded aspect of the scrape operation.
        known_users (set): Ensures no users are scraped twice.
        users_to_scrape (Queue): Controls which users are left to be scraped.
        bookface_network (dict): The results of the Bookface scraping process.
        user_info (dict): The user information for the authenticated user.

    """
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

        self.session = self.create_retry_session()
        self.session.headers.update(DEFAULT_REQUEST_HEADERS)
        self.authenticated = False

        self.pool = ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS)
        self.known_users = set()
        self.users_to_scrape = Queue()
        self.bookface_network = dict()

        logging.info("Authenticating session...")
        self.authenticate_session()

        if self.authenticated:
            logging.info("Authentication successful.")
            logging.info("Getting authenticated user information...")
            user_info_base64_string = self.get_user_info_base64_string()
            self.user_info = self.parse_user_info(user_info_base64_string)
            logging.info("Authenticated user information parsed successfully.")

            if self.user_info:
                logging.info("Getting followers for authenticated user...")
                self.user_info["followers"] = self.get_follower_ids()
                self.user_info["followers_count"] = len(self.user_info["followers"])
                logging.info("Follower list for authenticated user populated.")
            else:
                logging.warning("No user info for authenticated user, skipping get_followers.")

        else:
            logging.error("Session not authenticated.")

    def __del__(self):
        self.session.close()
        logging.info("Session closed.")

    @staticmethod
    def create_retry_session(number_of_retries: int=NUMBER_OF_RETRIES,
                             status_codes_to_retry: Tuple=ERROR_STATUS_CODES,
                             backoff_factor: float=BACKOFF_FACTOR) -> requests.Session:
        """
        Creates a session that handles connection issues using a retry mechanism.

        Args:
            number_of_retries: Maximum number of retries per request.
            status_codes_to_retry: HTTP response codes to initiate a retry for.
            backoff_factor: Controls the delay between requests in case a retry is needed.

        Returns:
            A session with an active retry mechanism for all HTTP traffic.
        """
        session = requests.Session()
        retry_controller = Retry(total=number_of_retries,
                                 read=number_of_retries,
                                 connect=number_of_retries,
                                 backoff_factor=backoff_factor,
                                 status_forcelist=status_codes_to_retry)

        http_adapter = HTTPAdapter(max_retries=retry_controller)
        session.mount("http://", http_adapter)
        return session

    @staticmethod
    def parse_user_info(user_info_string: str) -> Dict:
        """
        Decodes a base64-encoded Bookface user infomation string and extracts all available data in it.

        Args:
            user_info_string:

        Returns:
            A dictionary object containing the parsed Bookface user information.
        """
        user_info = dict()

        if not user_info_string:
            return user_info

        try:
            decoded_user_info_bytes = b64decode(user_info_string)

            user_info["decoded_user_info_string"] = str(decoded_user_info_bytes)

            user_id_last_char = decoded_user_info_bytes.find(USER_INFO_STRING_FIRST_NAME_SUFFIX)
            user_info["user_id"] = decoded_user_info_bytes[2:user_id_last_char].decode("utf-8")

            first_name_first_char = user_id_last_char + 2
            first_name_last_char = decoded_user_info_bytes.find(USER_INFO_STRING_LAST_NAME_SUFFIX)
            user_info["first_name"] = decoded_user_info_bytes[first_name_first_char:first_name_last_char].decode("utf-8")

            age_color_separator_position = decoded_user_info_bytes.find(USER_INFO_STRING_AGE_SUFFIX)

            last_name_first_char = first_name_last_char + 2
            last_name_last_char = age_color_separator_position - 2
            user_info["last_name"] = decoded_user_info_bytes[last_name_first_char:last_name_last_char].decode("utf-8")

            user_info["age"] = decoded_user_info_bytes[age_color_separator_position - 1]

            favorite_color_red = decoded_user_info_bytes[age_color_separator_position + 1]
            favorite_color_green = decoded_user_info_bytes[age_color_separator_position + 2]
            favorite_color_blue = decoded_user_info_bytes[age_color_separator_position + 3]
            favorite_color = (favorite_color_red, favorite_color_green, favorite_color_blue)
            user_info["favorite_color"] = favorite_color

            first_name_prefix_position = user_id_last_char + 1
            user_info["first_name_prefix"] = decoded_user_info_bytes[first_name_prefix_position]

            last_name_prefix_position = first_name_last_char + 1
            user_info["last_name_prefix"] = decoded_user_info_bytes[last_name_prefix_position]

            user_info["favorite_color_suffix"] = decoded_user_info_bytes[age_color_separator_position + 4]

        except Exception as ex:
            logging.error(f"Error in parse_user_info for string {user_info_string}: {ex}")

        return user_info

    @staticmethod
    def export_network_to_file(network_dict: Dict, filename: str):
        """
        Used mainly for testing, in order to save time - export scraped Bookface network for later inspection/analysis.

        Args:
            network_dict: The Bookface network object.
            filename: Output file name to export Bookface data to, JSON-formatted.
        """
        try:
            with open(filename, "w") as output_file:
                json.dump(network_dict, output_file)
                logging.info(f"Network exported to {filename}.")

        except Exception as ex:
            logging.error(f"Error while saving network (size: {len(network_dict)}) to file {filename}: {ex}")

    def import_network_from_file(self, filename: str):
        """
        Used mainly for testing, in order to save time - import previously exported Bookface network.

        Args:
            filename: Input file name to import Bookface data from, file should be JSON-formatted.
        """
        try:
            with open(filename, "r") as input_file:
                network_data = json.load(input_file)
                self.bookface_network = network_data
                logging.info(f"Network imported from {filename}.")

        except Exception as ex:
            logging.error(f"Error while reading network from file {filename}: {ex}")

    def authenticate_session(self):
        """
        Send authentication request to Bookface API.
        If successful, session headers will be updated and the client's 'authenticated' member will be set to True.
        """
        login_headers = dict()
        login_headers["Content-Type"] = "application/json;charset=UTF-8"
        login_headers["Origin"] = HOST
        login_headers["Referrer"] = f"{HOST}/login"

        login_payload_str = f'{{"username":\"{self.username}\","password":\"{self.password}\"}}'

        try:
            login_response = self.session.post(LOGIN_API_URL, headers=login_headers, data=login_payload_str)
            if login_response.status_code == OK_STATUS_CODE:
                if AUTH_HEADER_NAME in login_response.headers.keys():
                    authentication_header = {AUTH_HEADER_NAME: login_response.headers[AUTH_HEADER_NAME]}
                    self.session.headers.update(authentication_header)
                    self.authenticated = True
                else:
                    logging.error(f"Authentication error for user {self.username}: {login_response.text}")

        except RetryError as err:
            logging.error(f"Max number of retries in authenticate_session exceeded: {err}")

        except Exception as ex:
            logging.error(f"Unexpected error in authenticate_session for user {self.username}: {ex}")

    def get_user_info_base64_string(self, user_id: str=None) -> str:
        """
        Get user information (encoded in base64) from Bookface servers.

        Args:
            user_id: Bookface user id, user None for authenticated user.

        Returns:
            A string containing the Bookface user information encoded in base64.
        """
        referrer_header_value = HOST
        if not user_id:
            user_id = "me"
        else:
            referrer_header_value = f"{referrer_header_value}/user/{user_id}"

        api_call = f"{USER_API_URL}/{user_id}"
        referrer_header = {"Referrer": referrer_header_value}

        try:
            response = self.session.get(api_call, headers=referrer_header)
            if response.status_code == OK_STATUS_CODE:
                if response.text:
                    return response.text

        except RetryError as err:
            logging.warning(f"Max number of retries in get_user_info_base64_string for user {user_id} exceeded: {err}")

        except Exception as ex:
            logging.error(f"Unexpected error in  get_user_info_base64_string for user {user_id}: {ex}")

        logging.warning(f"No user information for user {user_id}.")
        return ""

    def get_user_info(self, user_id: str=None) -> Dict:
        """
        Main function for the home task - create an authenticated Bookface client and scrape the available network.

        Args:
            user_id: Bookface user id, use None for authenticated user.

        Returns:
            A dictionary object containing the scraped Bookface user information.
        """

        if user_id:
            logging.debug(f"Getting user information for user {user_id}...")
            user_info_string = self.get_user_info_base64_string(user_id)
            user_info_dict = self.parse_user_info(user_info_string)
            logging.debug(f"User information for user {user_id} parsed successfully.")
            return user_info_dict
        else:
            return self.user_info

    def get_follower_ids(self, user_id: str=None) -> List:
        """
        Get the Bookface follower ids for a specific user.

        Args:
            user_id: Bookface user id, use None for authenticated user.

        Returns:
            A list object containing the ids of the user's followers.
        """
        referrer_header_value = HOST
        if not user_id:
            user_id = "me"
        else:
            referrer_header_value = f"{referrer_header_value}/user/{user_id}"

        referrer_header = {"Referrer": referrer_header_value}
        base_api_call = f"{USER_API_URL}/{user_id}/{FOLLOWERS_BASE_API_CALL}"

        has_more_followers = True
        followers_index = 0
        followers_list = list()
        try:
            while has_more_followers:
                followers_response = self.session.get(f"{base_api_call}{followers_index}", headers=referrer_header)

                if followers_response.status_code == OK_STATUS_CODE:
                    current_followers = json.loads(followers_response.text)["followers"]
                    current_followers_ids = ([follower["id"] for follower in current_followers])
                    followers_list += current_followers_ids

                    has_more_followers = json.loads(followers_response.text)["more"]
                    followers_index += 10

        except RetryError as err:
            logging.error(f"Max number of retries in get_follower_ids for user {user_id} exceeded: {err}")

        except Exception as ex:
            logging.error(f"Unexpected error in get_follower_ids for user {user_id} and index {followers_index}: {ex}")
            return followers_list

        return followers_list

    def populate_bookface_network(self):
        """
        Adds the authenticated user's followers to the scraping queue, and starts scraping Bookface.
        Scraping is performed in parallel, and updates are logged after each successful user scrape.
        """
        logging.info("Adding authenticated user's followers to scrape queue...")
        for follower_id in self.user_info["followers"]:
            self.users_to_scrape.put(follower_id)

        while True:
            try:
                current_user_id = self.users_to_scrape.get(timeout=QUEUE_TIMEOUT)
                if current_user_id not in self.known_users:
                    logging.debug(f"Creating scrape thread for user {current_user_id}...")
                    self.known_users.add(current_user_id)
                    job = self.pool.submit(self.scrape_bookface_user, current_user_id)
                    job.add_done_callback(self.log_scraping_status)

            except Empty:
                logging.debug("Finished populating threads.")
                break

            except Exception as ex:
                logging.warning(f"Unexpected error while scraping Bookface: {ex}")
                continue

        self.pool.shutdown(wait=True)

    def scrape_bookface_user(self, user_id: str) -> Dict:
        """
        Main function (job) to be run when scraping Bookface.
        Gets user information and followers and adds new users to scrape queue.

        Args:
            user_id: Bookface user id.

        Returns:
            A dictionary object containing the scrape status regarding this specific user.
        """
        logging.debug(f"Getting user information for user {user_id}")
        user_info = self.get_user_info(user_id)
        logging.debug(f"Getting followers for user {user_id}")
        user_info["followers"] = self.get_follower_ids(user_id)
        user_info["followers_count"] = len(user_info["followers"])
        self.bookface_network[user_id] = user_info

        logging.debug(f"Adding new users from {user_id}'s followers to scrape queue...")
        new_users = list()
        for follower_id in user_info["followers"]:
            if follower_id not in self.known_users:
                self.users_to_scrape.put(follower_id)
                new_users.append(follower_id)

        return {"new_users_count": len(new_users),
                "full_name": f"{user_info['first_name']} {user_info['last_name']}",
                "followers_count": len(user_info['followers'])}

    def log_scraping_status(self, function_future: Future):
        """
        Scrape function callback - log status updates to console while scraping Bookface.

        Args:
            function_future: The job item that's created when submitting a job to a pool. Contains the job's result.
        """
        function_result_dict = function_future.result()
        logging.info(f"known users: {len(self.known_users)}, "
                     f"scraepd network_size: {len(self.bookface_network)}, "
                     f"{function_result_dict['full_name']}, "
                     f"new users count: {function_result_dict['new_users_count']}, "
                     f"followers count: {function_result_dict['followers_count']}")

    def calculate_following(self):
        """
        Calculate the following list for each available Bookface user and add it to the existing network data.
        """
        all_following = dict()
        if self.bookface_network:
            logging.info("Creating following list...")
            for following_user_id, following_user_info in self.bookface_network.items():
                following_list = [scraped_user_id for scraped_user_id, scraped_user_info in self.bookface_network.items()
                                  if following_user_id in scraped_user_info["followers"]]
                all_following[following_user_id] = following_list

            logging.info("Updating Bookface network dictionary...")
            for current_user_id, current_user_info in all_following.items():
                self.bookface_network[current_user_id]["following"] = current_user_info
                self.bookface_network[current_user_id]["following_count"] = len(current_user_info)

            logging.info("Following calculation completed successfully.")
        else:
            logging.warning("No network available for following calculation.")


def get_bookface_network(username: str, password: str, from_file: str=None, to_file: str=None):
    """
    Main function for the home task - create an authenticated Bookface client and scrape the available network.

    Args:
        username: The first parameter.
        password: The second parameter.
        from_file: Name of an existing file to load Bookface data from.
        to_file: Name of a file to write Bookface data to.

    Returns:
        A JSON-formatted object containing the scraped Bookface network data.
    """
    logging.basicConfig(level=LOG_LEVEL)
    bookface_client = BookfaceClient(username, password)

    if bookface_client.authenticated:
        if from_file:
            bookface_client.import_network_from_file(from_file)
        else:
            bookface_client.populate_bookface_network()

        bookface_client.calculate_following()

        if to_file:
            bookface_client.export_network_to_file(bookface_client.bookface_network, to_file)

        return json.dumps(bookface_client.bookface_network)

    return None


u = DEFAULT_USERNAME
p = DEFAULT_PASSWORD
bookface_network = get_bookface_network(u, p)

# TODO: your code here
