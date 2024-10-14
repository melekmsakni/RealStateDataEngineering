import json
import requests
import logging
import fastavro
from confluent_kafka import Producer
from io import BytesIO
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
import requests
from bs4 import BeautifulSoup
import time 

logging.basicConfig(
    level=logging.ERROR,                      # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
    handlers=[
        logging.FileHandler("tayara_ingesting.log"),      # Log to a file named 'app.log'
        logging.StreamHandler()              # Optionally log to console as well
    ]
)


logger = logging.getLogger(__name__)

# Configuration
KAFKA_TOPIC = "tayara_topic"
KAFKA_SERVER = "kafka-broker:29092"
SCHEMA_REGISTRY_SUBJECT = "RealState-schema"
# SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


url = "https://www.tayara.tn/api/marketplace/search-api/"

headers = {
  'accept': 'application/json',
  'accept-language': 'en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7,ar;q=0.6',
  'content-type': 'application/json',
  'cookie': 'caravel-cookie="902a9009ddae8717"; caravel-cookie="97761743e959360e"; _gcl_au=1.1.1486801219.1727114978; load-heavy-content=true; rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX19mExdMXxw0rHQD%2BxnFft0YAHSlUTCztMllkwYcG%2BhuAXA2VwunqKIs; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX1%2FF6oTwlXGSuyupLQRoSVw%2F5OZa4%2FDdpBoSCnXfc9tRUtZauWsWCnGD; rs_ga=GA1.1.1858177586.1727114984; caravel-cookie="902a9009ddae8717"; userid=3fca9a5c-0927-4be4-9753-8fde730d2d84; _ga_93M2KYSFK9=GS1.1.1727490387.9.0.1727490387.60.0.0; _ga=GA1.1.1858177586.1727114984; rl_user_id=RudderEncrypt%3AU2FsdGVkX1%2FRuufAU7DskfIDt6%2FbThceJd5yj6Ppw8VCWBeNMYhRW6XbuwyfiKXv9B2yWMggAKeZmcg7NxJX1A%3D%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX1%2BmE2InvX5vrEEK8%2F%2FjRcLRW9PemlgiAWT6fCR3ue0UAQ1LaW%2BAN5A%2BrLkpJT%2BQKtvtytxxOBScKQ%3D%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX1%2Bw4WDjTish%2FkCKzKWqxh6G2lRmIkcVyrw%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX1%2BkfSrWRCtFrx4VNs%2FuxVp6tdy5JQwY2%2FT5odfiGxYuJRg4Ju964rcRcGTXRnsibcdEYuhSya9JD1mLhRlms7JNSWFpgFSrLb4%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX19qqPxTz2AImTN5TveWo%2B%2FRu%2B%2FsM8aSuvo%3D; rl_session=RudderEncrypt%3AU2FsdGVkX1%2B1Z1h6l1bh5hIGT%2FMQ6MNabNVnbS84SK6L6Xenes0wsVHCHCZEU9nr4mTlryNpQtUqUMNjqeqcbWb92r0wYH5PXqlr7zrWj%2BIohwkWGqU5fBnzwgBTy8UHdyYpt%2Fwt3d9Ihg5a8q5X2g%3D%3D; _ga_0LMQZK4Q75=GS1.1.1728924692.22.1.1728924845.38.0.0; rs_ga_93M2KYSFK9=GS1.1.1728924692.29.1.1728924845.38.0.0',
  'origin': 'https://www.tayara.tn',
  'priority': 'u=1, i',
  'referer': 'https://www.tayara.tn/ads/c/Immobilier/Appartements/?Type+de+transaction=%C3%80+Louer&page=1',
  'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}


property_categories = {
    "À Louer": "rent",
    # 'À Vendre':'sale'
}
property_types = {
    # "60be84be50ab95b45b08a09f": "office",
    # '60be84bd50ab95b45b08a09d':'villa',
    '60be84bd50ab95b45b08a09c':'apartment',
    # '60be84be50ab95b45b08a0a0':'commercial',
    # '60be84be50ab95b45b08a0a1':'land',
}
KAFKA_TOPIC = "tayara_topic_apartment_louer"

def delivery_report(errmsg, msg):
    if errmsg is not None:
        logger.error("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    logger.info(
        "Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def get_item_specific_data(item):
    id_item = item["id"]

    url = f"https://www.tayara.tn/item/{id_item}"

    payload = {}
    headers = {
        "Referer": "https://www.tayara.tn/ads/c/Immobilier/Maisons%20et%20Villas/?page=1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }

    specific_data = {"rooms": None, "bathrooms": None, "surface": None}

    def get_data_html(html):
        try:
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the <script> tag with the id "__NEXT_DATA__"
            script_tag = soup.find("script", id="__NEXT_DATA__")

            # Load the JSON data from the script tag
            json_data = json.loads(script_tag.string)

            # Extract the 'props' section from the JSON data
            props_data = json_data.get("props")

            pageProps = props_data.get("pageProps")

            adDetails = pageProps.get("adDetails")

            adParams = adDetails.get("adParams")
            for param in adParams:

                label = param["label"]
                value = param["value"]

                if label == "Salles de bains":
                    specific_data["bathrooms"] = int(value) if value else None
                elif label == "Chambres":
                    specific_data["rooms"] = int(value) if value else None
                elif label == "Superficie":
                    specific_data["surface"] = float(value) if value else None

            return specific_data

        except Exception as e:
            logger.error(f"{id_item} - error while parsing html data : {e}")
            return {}


    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:

            specific_data = get_data_html(response.text)
            return specific_data
        else:
            logger.error(f"{id_item} - couldn't get response ")
            return {}

    except Exception as e:
        logger.error(f"{id_item} -  problem with api request : {e} ")
        return {}


def preprocess_data(item_data):
    item, property_type, property_category = item_data

    specific_data = get_item_specific_data(item)

    data = item
    item_dict = {
        "id": data["id"],
        "title": data["title"],
        "description": data["description"],
        "price": float(data["price"]),
        "delegation": data["location"]["delegation"],
        "governorate": data["location"]["governorate"],
        "publishedOn": data["metadata"]["publishedOn"],
        "isShop": data["metadata"]["publisher"]["isShop"],
        "publisher": data["metadata"]["publisher"]["name"],
        "images": data["images"],
        "category": property_categories[property_category],
        "type": property_types[property_type],
    }

    item_dict.update(specific_data)

    return item_dict


def serilize_and_send_kafka(processed_data, producer):

    try:
        
        producer.produce(
            topic=KAFKA_TOPIC, value=processed_data , key=processed_data['id'] , on_delivery=delivery_report
        )

        # Trigger any available delivery report callbacks from previous produce() calls
        events_processed = producer.poll(1)
        # logger.info(f"events_processed: {events_processed}")

        # Ensure messages are being sent in batches and check queue status
        messages_in_queue = producer.flush(1)
        # logger.info(f"messages_in_queue: {messages_in_queue}")

    except Exception as e:
        logger.error(f"{processed_data['id']} -  Error producing message: {e}")


def preprocess_and_send_to_kafka(item_data, producer):
    processed_data = preprocess_data(item_data)

    serilize_and_send_kafka(processed_data, producer)
    time.sleep(0.5)


def tayara_get_data(property_type, property_category, producer):
    payload = json.dumps(
        {
            "searchRequest": {
                "query": "",
                "offset": 0,
                "limit": 9999,
                "sort": 0,
                "filter": {
                    "categoryId": "60be84bc50ab95b45b08a093",
                    "subCategoryId": property_type,
                    "adParamsMap": {"Type de transaction": property_category},
                    "rangeAdParamsMap": {},
                    "governorate": "",
                    "delegation": [""],
                    "minPrice": 0,
                    "maxPrice": 0,
                    "level": 0,
                    "state": 2,
                },
            }
        }
    )

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code != 200:
            logger.error(
                f"Failed to get data from Tayara API {property_type}, {property_category}. Status: {response.status_code}"
            )
            return None

        response_json = json.loads(response.text)
        data = response_json[0][0]

        for item in data:
            item_data = (item, property_type, property_category)

            preprocess_and_send_to_kafka(item_data, producer)
            

    except Exception as e:
        logger.error(
            f"Error occurred while sending request to Tayara API {property_type}, {property_category}: {e}"
        )
        return


def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({"url": schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version


def tayara_all_data():

    sr, latest_version = get_schema_from_schema_registry(
        SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_SUBJECT
    )
    value_avro_serializer = AvroSerializer(
        schema_registry_client=sr,
        schema_str=latest_version.schema.schema_str,
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "security.protocol": "plaintext",
            "value.serializer": value_avro_serializer,
            "delivery.timeout.ms": 120000,  # set it to 2 mins
            "enable.idempotence": "true",
        }
    )

    for property_type in property_types:
        logger.info(f"Starting scraping for type {property_types[property_type]}")
        for property_category in property_categories:
            tayara_get_data(property_type, property_category, producer)

    producer.flush()


# tayara_all_data()

# adDeleted:5 => sold 
# adDeleted:4 => deleted 
