import http.client
from kafka import KafkaProducer
import json
import time
import http.client
import sys
region_code = "GT"


def get_data(producer):
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
    conn.request(
        "GET",
        f"/api/estates/search?contract=acquis&province={region_code}&sector=res&type=&page=0&section=estate",
        headers=headers,
    )
    response = conn.getresponse()

    response_content = response.read()

    # Decode the response content if it's in bytes
    response_text = response_content.decode("utf-8")
    # Parse the response text as JSON
    response_data = json.loads(response_text)

    # Print the parsed JSON data
    data = response_data["estates"]

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

    print(f"sending data to kafka")
    for data in filtered_data_list:
        # print(data)
        message_size_bytes = sys.getsizeof(data)
        print(f"Message size: {message_size_bytes} bytes")
        producer.send("properties", value=json.dumps(data).encode("utf-8"))
        print("data sent to kafka")
        time.sleep(1)

    # with open ('/home/melek/Desktop/RealStateDataEngineering/output2.json','w',encoding='utf-8') as f:
    #     json.dump(filtered_data_list,f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    data = get_data(producer)
