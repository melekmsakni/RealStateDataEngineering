import http.client
from kafka import KafkaProducer
import json
import time
import http.client
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = http.client.HTTPSConnection("www.tecnocasa.tn")
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

region_code = "GT"
Property_Category="acquis"


def preProcess_and_send_to_kafka(data, producer,i=0):
    keys_to_keep = [
        "ad_type",
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
        item["images_list"] = list(item["images"][0].values())
        del item["images"]

    for data in filtered_data_list:
        producer.send("properties", value=json.dumps(data).encode("utf-8"))

        # putiing data into json file
    # with open(
    #     f"/home/melek/Desktop/RealStateDataEngineering/output_simple_{i}.json",
    #     "w",
    #     encoding="utf-8",
    # ) as f:
    #     json.dump(filtered_data_list, f, ensure_ascii=False, indent=4)


def getting_total_number_of_pages():
    try:
        conn.request(
            "GET",
            f"/api/estates/search?contract={Property_Category}&province={region_code}&sector=res&type=&page=0&section=estate",
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
        logger.info(f'number of pages for region {region_code}  is {number_of_pages}')
        return number_of_pages

    except Exception as e:
        logger.error(
            f"Error occurred while sending request to get Total number of pages : {e}"
        )
        return None


def send_request_tecnocasa(region_code, page_number=0):

    try:
        conn.request(
            "GET",
            f"/api/estates/search?contract={Property_Category}&province={region_code}&sector=res&type=&page={page_number}&section=estate",
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

        data = response_data["estates"]

        return data

    except Exception as e:
        logger.error(f"Error occurred while sending request to Tecnocasa API: {e}")
        return None


def tecnocasa_data(producer):

    number_of_pages = getting_total_number_of_pages()

    if number_of_pages == None:
        return None

    for page in range(number_of_pages + 1):
        logger.info(f"Fetching data for page {page}...")
        data = send_request_tecnocasa(region_code="GT", page_number=page)
        if data:
            preProcess_and_send_to_kafka(data, producer)
        time.sleep(3)


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    tecnocasa_data(producer)
