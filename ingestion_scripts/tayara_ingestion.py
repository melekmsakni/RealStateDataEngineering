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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_TOPIC = "tayara_topic"
KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_SUBJECT = "RealState-schema"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


url = "https://www.tayara.tn/api/marketplace/search-api/"

headers = {
    "accept": "application/json",
    "accept-language": "en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7,ar;q=0.6",
    "content-type": "application/json",
    "cookie": 'rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX18uZyFbSMQDoGuptOWRlEySJTZEympbxzM%3D; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX1845Ob5K%2FFK5uHYDXA6%2BtDCNxF4Qa1JIu4%3D; _gcl_au=1.1.430962370.1726233850; rs_ga=GA1.1.887514326.1726233851; caravel-cookie="5e53e2227ef8cc3b"; _gid=GA1.2.633884130.1726233934; _ga=GA1.1.887514326.1726233851; _ga_0LMQZK4Q75=GS1.1.1726257232.3.1.1726258721.60.0.0; rl_session=RudderEncrypt%3AU2FsdGVkX19ewqLmlDRLtjVDGyJ3iwLGBianSFeRRes2bv6J9QwNFl502CMUnD%2FoxXpBfrH4AeYU%2BC4xnMZrcxJa%2BJAvwqH2VzMHVNE4ahJYcvMrOxJ2ai3%2FZysrvezVnSjeepCAm0kLgqOCJgkTsg%3D%3D; rl_user_id=RudderEncrypt%3AU2FsdGVkX19BHEyDaj0xPylDPLyETNzgcQrMAEjhUgc%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX1%2FILIvWT8fVZAgJTTeCL1RoNq0y5eK%2F9EK%2FCxlQ0LiqaYyHa1rFhf5WwDS%2FBoIYmwpOhIaDRe7PJA%3D%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX19Vx3nBRAosNLHpQuc%2FJO6mm5OVe40SB1s%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX1%2B6oatwnagFQ65yglOH7B%2FF0hZ5F2DtobY%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX19vh6QLSMeXVrJes%2B5wn%2Bx8kObCN3HQEyY%3D; rs_ga_93M2KYSFK9=GS1.1.1726257228.3.1.1726258722.58.0.0',
    "origin": "https://www.tayara.tn",
    "priority": "u=1, i",
    "referer": "https://www.tayara.tn/ads/c/Immobilier/Maisons%20et%20Villas/?Type+de+transaction=%C3%80+Vendre&page=2",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
}


property_categories = {
    "À Louer": "rent",
    # 'À Vendre':'sale'
}
property_types = {
    "60be84be50ab95b45b08a09f": "office",
    # '60be84bd50ab95b45b08a09d':'villa',
    # '60be84bd50ab95b45b08a09c':'apartment',
    # '60be84be50ab95b45b08a0a0':'commercial',
    # '60be84be50ab95b45b08a0a1':'land',
}


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

            pageProps = props_data.get("pageProps", {})

            adDetails = pageProps.get("adDetails", [])

            adParams = adDetails.get("adParams", [])
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
            logger.error(f"error while parsing html data : {e}")
            return {}

        # logger.info(json.dumps(props_data, indent=2))  # Print formatted JSON

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:

            specific_data = get_data_html(response.text)
            return specific_data
        else:
            logger.error(f"couldn't get response ")
            return {}

    except Exception as e:
        logger.error(f"problem with api request : {e} ")
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
            topic=KAFKA_TOPIC, value=processed_data , on_delivery=delivery_report
        )

        # Trigger any available delivery report callbacks from previous produce() calls
        events_processed = producer.poll(1)
        logger.info(f"events_processed: {events_processed}")

        # Ensure messages are being sent in batches and check queue status
        messages_in_queue = producer.flush(1)
        logger.info(f"messages_in_queue: {messages_in_queue}")

    except Exception as e:
        logger.info(f"Error producing message: {e}")


def preprocess_and_send_to_kafka(item_data, producer):
    processed_data = preprocess_data(item_data)

    serilize_and_send_kafka(processed_data, producer)


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
                f"Failed to get data from Tayara API. Status: {response.status_code}"
            )
            return None

        response_json = json.loads(response.text)
        data = response_json[0][0]

        for item in data[:1]:
            item_data = (item, property_type, property_category)

            preprocess_and_send_to_kafka(item_data, producer)
            logger.info("item sent to kafka ")

        logger.info("Data successfully sent to Kafka")

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


tayara_all_data()
