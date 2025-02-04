import http.client
from kafka import KafkaProducer
import json
import time
import http.client
import sys
import logging
import concurrent.futures
import random
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# stagger_delay = random.uniform(1, 3)
stagger_delay = 0.5


conn = http.client.HTTPSConnection("www.tecnocasa.tn")

# testing='_aut'

KAFKA_TOPIC = f"tecnocasa_topic"
# KAFKA_SERVER = "kafka-broker:29092"
KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_SUBJECT = "RealState-schema"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7,ar;q=0.6",
    "cookie": "_ga=GA1.2.2101393.1716678421; _gid=GA1.2.1922791699.1717081469; recent_estate=%5B42395%2C42228%2C41542%2C38446%2C40103%2C39235%2C30625%2C27593%2C33648%2C39595%5D; _ga_EN93HC78MW=GS1.2.1717085048.11.1.1717085120.0.0.0; XSRF-TOKEN=eyJpdiI6Ijl3Nms0VzNwcVlYS3NCdHhCTEZzY0E9PSIsInZhbHVlIjoiYS84c2p5TXkwUkhXaS9ZOEtqN3JaTHNDZVFVTnc1VDV2NzdoWEF0OGEzbjdzb05QUUNVUml3M2xJcFJac1hrRnMxbElrS0pHRjllUTQzdGZkY1p5cVZKY1pOMkV2c09jbFI2R0FZSEpIZ0hDNFh0WGNqTHNRekNwa2JGRjkzZkkiLCJtYWMiOiI2MzNhMjk5NWFiM2RiMWNjNTgyZDQ1YjgyNmMyYmMyMDdjNjhhMDFhNDgyYWI0YWZkMWM4ZWQxNmZlYzM3NWI3IiwidGFnIjoiIn0%3D; portale_tecnocasa_session=eyJpdiI6InhUT1ozcGNhQmdZdkJDczZuT2lVV1E9PSIsInZhbHVlIjoiQzNuWExDMS8yaFdmZ3FEL3FaV0FseHVhMEVLTHBHaHFmNFRLajNsZHRmaHVJam9mcnozZnlKaXk2cVNoUWY5anh0TFBqTDJZNFM5REkwOU0vK3BwYVZWd1psWDEybEREQnZ5MUJVK3g0U2l3OFFMRng1djFKdWhQYS9oRkVuajgiLCJtYWMiOiIzMTA1YzA1YzlkOWM1ZGYyNjRhYzg3ZTgyNjA1MDkyMzM1NTViM2I4ZmMxNDUzZDdjYWEzMWE2YTAwMGVmNDZkIiwidGFnIjoiIn0%3D",
    "referer": "https://www.tecnocasa.tn/vendre/immeubles/nord-est-ne/grand-tunis.html/pag-20",
    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "x-xsrf-token": "eyJpdiI6Ijl3Nms0VzNwcVlYS3NCdHhCTEZzY0E9PSIsInZhbHVlIjoiYS84c2p5TXkwUkhXaS9ZOEtqN3JaTHNDZVFVTnc1VDV2NzdoWEF0OGEzbjdzb05QUUNVUml3M2xJcFJac1hrRnMxbElrS0pHRjllUTQzdGZkY1p5cVZKY1pOMkV2c09jbFI2R0FZSEpIZ0hDNFh0WGNqTHNRekNwa2JGRjkzZkkiLCJtYWMiOiI2MzNhMjk5NWFiM2RiMWNjNTgyZDQ1YjgyNmMyYmMyMDdjNjhhMDFhNDgyYWI0YWZkMWM4ZWQxNmZlYzM3NWI3IiwidGFnIjoiIn0=",
}
regions_code = {
    "Cap Bon": "NB",
    "Grand Tunis": "GT",
    "Kairouan": "KA",
    "Mahdia": "MH",
    "Monastir": "MS",
    "Sfax": "SF",
    "Sousse": "SS",
}
custom_region_mapping = {
    "ain-zaghouan": "tunis",
    "aouina": "tunis",
    "ariana": "ariana",
    "ben-arous": "ben arous",
    "borj-cedria": "ben arous",
    "bou-mhel-el-bassatine": "ben arous",
    "carthage": "tunis",
    "centre-urbain-nord": "tunis",
    "cite-el-khadra": "tunis",
    "el-menzah": "tunis",
    "el-mourouj": "ben arous",
    "el-omrane": "tunis",
    "ennasr": "ariana",
    "ezzahra": "ben arous",
    "gammarth": "tunis",
    "hammam-lif": "ben arous",
    "jardins-d-el-menzah": "tunis",
    "jardins-de-carthage": "tunis",
    "la-goulette": "tunis",
    "la-manouba": "manouba",
    "la-marsa": "tunis",
    "la-soukra": "ariana",
    "lafayette-centre-ville": "tunis",
    "le-bardo": "tunis",
    "le-kram": "tunis",
    "les-berges-du-lac": "tunis",
    "manar": "tunis",
    "medina-jedida": "ben arous",
    "megrine": "ben arous",
    "mnihla": "ariana",
    "mornag": "ben arous",
    "mutuelleville": "tunis",
    "rades": "ben arous",
    "raoued": "ariana",
    "sidi-bou-said": "tunis",
    "sidi-thabet": "ariana",
    "zone-industrielles-nord": "tunis",
    "zone-industrielles-sud": "tunis",
    "hammamet": "nabeul",
    "nabeul": "nabeul",
}


# for ech region we have 4 possibilities :
#     rg acquis  Res
#     rg acquis  ind
#     rg locazi  res
#     rg locazi  ind

combinations = [
    ("acquis", "res"),
    ("acquis", "ind"),
    ("locazi", "res"),
    ("locazi", "ind"),
]


# region_code = regions_code["Grand Tunis"]
# Property_category = "acquis"  # or "locazi"
# property_type = "res"  # or 'ind'
def exponential_backoff_request(
    request_func, max_retries=5, initial_delay=1, backoff_factor=2, *args, **kwargs
):
    """
    Exponential backoff for retrying a request.

    Args:
    - request_func: The function to be retried.
    - max_retries: The maximum number of retries.
    - initial_delay: The initial delay between retries in seconds.
    - backoff_factor: The factor by which the delay increases after each retry.
    - args, kwargs: Arguments for the request function.

    Returns:
    - The response from the request function if successful, or None if all retries fail.
    """
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            response = request_func(*args, **kwargs)
            return response
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"All {max_retries} attempts failed.")
                return None

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({"url": schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def preprocess_tecnocasa(data):
    try:
        """
        Preprocess data from Tecnocasa API.
        """

        def extract_digit(num):
            if num is None or num == "null":
                return None

            num = "".join(char for char in num if char.isdigit() or char == ".")
            # Convert to float
            if not num:
                return None
            try:
                price = float(num)
            except ValueError:
                return None
            return price

        # Example usage
        def extract_surface(surface_str):
            if not surface_str:
                return None
            surface_numeric = surface_str.split()[0]
            surface = float(surface_numeric)
            return surface

        def extract_details_from_url(url):
            try:
                # Split the URL by '/'
                parts = url.split("/")

                # Extract the relevant parts
                category = parts[3]
                type_ = parts[4]
                region = parts[5]
                city = parts[6]

                return category, type_, region, city
            except Exception as e:
                logging.error(f"error while extracting data from url :{e}")
                return None

        def get_specific_region(city):
            return custom_region_mapping.get(city)

        property_category, property_type, region, city = extract_details_from_url(
            data.get("detail_url")
        )

        if region == "grand-tunis" or region == "cap-bon":
            region = get_specific_region(city)

        tecnocasa_payload = {
            "id": str(data.get("id")),
            "title": data.get("title"),
            "category": property_category,
            "region": region,
            "city": city,
            "source": "tecnocasa",
            "link": data.get("detail_url"),
            "type": property_type,
            "surface": extract_surface(data.get("surface")),
            "rooms": extract_digit(data.get("rooms")),
            "price": extract_digit(data.get("price")),
            "images": list(data.get("images")[0]["url"].values()),
            "timestamp": datetime.now().isoformat(),
            "source_specific_data": {
                "subtitle": data.get("subtitle"),
                "bathrooms": (
                    str(data.get("bathrooms")) if data.get("bathrooms") else None
                ),
                "publisher": data["agency"].get("id"),
                "discount": data.get("discount"),
                "is_discounted": data.get("is_discounted"),
                "previous_price": data.get("previous_price"),
                "discount_percentage": data.get("discount_percentage"),
                "exclusive": data.get("exclusive"),
                "virtual_tour": data.get("virtual_tour"),
            },
        }

        return tecnocasa_payload

    except Exception as e:
        logging.error(f"erooor in  preprocessing fn : {e}")
        return None


def send_to_kafka(topic, payload, producer):
    """
    Send the processed payload to Kafka.
    """
    try:
        producer.send(topic, value=json.dumps(payload).encode("utf-8"))
        logging.info(f"Succcess to Kafka topic {topic}")
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")


def preprocess_and_send_to_kafka(data_list, producer):
    """
    Preprocess the raw data and send to Kafka.
    """
    for data in data_list:

        try:
            processed_data = preprocess_tecnocasa(data)
            send_to_kafka(KAFKA_TOPIC, processed_data, producer)
        except Exception as e:
            logging.error(f"Error in preprocessing or sending data: {e}")


# this is an old version of the code that it used to send  data to kadfka
def preProcess_and_send_to_kafka_old_version(data, producer):
    raw_data = data
    tecnocasa_payload = {}
    tecnocasa_payload["id"] = str(data["id"])
    tecnocasa_payload["title"] = data["title"]

    keys_to_keep = [
        "detail_url",
        "images",
        "surface",
        "price",
        "rooms",
        "subtitle",
        "title",
    ]

    # Create new list of dictionaries with only desired keys

    filtered_data_list = [
        {
            key: (
                item[key]
                if key != "images"
                else [image["url"] for image in item["images"]]
            )
            for key in keys_to_keep
            if key in item
        }
        for item in data
    ]

    for item in filtered_data_list:

        item["region"] = "re"
        item["type"] = (
            "terrain villa appartement burau local_commercial "  # we can get this from the link
        )
        item["category"] = " rental sell "  # we get this from the api
        item["website"] = "tecnocasa"

        item["images_list"] = list(item["images"][0].values())
        del item["images"]

        item["location"] = item["subtitle"]
        del item["subtitle"]

        item["link"] = item["detail_url"]
        del item["detail_url"]

    for data in filtered_data_list:
        break
        producer.send("properties_tecnocasa", value=json.dumps(data).encode("utf-8"))

        # putiing data into json file
    with open(
        f"/home/melek/Desktop/RealStateDataEngineering/output_simple_tecnocasa.json",
        "a",
        encoding="utf-8",
    ) as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=4)


def getting_total_number_of_pages(region_code, property_category, property_type):
    try:
        conn.request(
            "GET",
            f"/api/estates/search?contract={property_category}&province={region_code}&sector={property_type}&type=&page=0&section=estate",
            headers=headers,
        )
        response = conn.getresponse()

        if response.status != 200:
            logger.error(
                f"Failed to get Total number of pages from Tecnocasa API. Status: {response.status}"
            )
            return None

        response_content = response.read()

        # Decode the response content if it's in bytes
        response_text = response_content.decode("utf-8")
        # Parse the response text as JSON
        response_data = json.loads(response_text)

        number_of_pages = response_data["pagination"]["total_pages"]
        logger.info(
            f"number of pages for region {region_code, property_category, property_type}  is {number_of_pages}"
        )
        return number_of_pages

    except Exception as e:
        logger.error(
            f"Error occurred while sending request to get Total number of pages for {region_code, property_category, property_type} : {e}"
        )
        return None


def send_request_tecnocasa(
    region_code, property_category, property_type, page_number=0
):

    try:
        conn.request(
            "GET",
            f"/api/estates/search?contract={property_category}&province={region_code}&sector={property_type}&page={page_number}&section=estate",
            headers=headers,
        )
        response = conn.getresponse()

        if response.status != 200:
            logger.error(
                f"Failed to get data from Tecnocasa API. Status: {response.status}"
            )
            return None

        response_content = response.read()

        # Decode the response content if it's in bytes
        response_text = response_content.decode("utf-8")
        # Parse the response text as JSON
        response_data = json.loads(response_text)

        data_list = response_data["estates"]
        if data_list == None:
            return []

        return data_list

    except Exception as e:
        logger.error(
            f"Error occurred while sending request to Tecnocasa API {region_code, property_category, property_type}: {e}"
        )
        return None


def tecnocasa_get_region_data(region_code, property_category, property_type, producer):

    number_of_pages = getting_total_number_of_pages(
        region_code, property_category, property_type
    )

    if number_of_pages == None:
        return None

    for page in range(number_of_pages + 1):

        data_list = send_request_tecnocasa(
            region_code, property_category, property_type, page_number=page
        )
        print(data_list)
        exit()

        if data_list != None and len(data_list) != 0:
            logger.info(
                f"{region_code, property_category, property_type}.... data fetched for page {page}..."
            )
            preprocess_and_send_to_kafka(data_list, producer)

        time.sleep(stagger_delay)


def tecnocasa_all_data():
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
    

    for region_code in list(regions_code.values())[:1]:
        logger.info(f"starting threads for region {region_code}")
        for property_category, property_type in combinations[:1]:
            tecnocasa_get_region_data(region_code, property_category, property_type, producer)


tecnocasa_all_data()
