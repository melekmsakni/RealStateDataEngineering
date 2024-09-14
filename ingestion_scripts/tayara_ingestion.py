import http.client
from kafka import KafkaProducer
import json
import time
import http.client
import sys
import logging
import concurrent.futures
import random
import threading
from datetime import datetime
import http.client
import json
import requests
import json



logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
# stagger_delay = random.uniform(1, 3)
stagger_delay = 0.5
semaphore = threading.Semaphore(1)



KAFKA_TOPIC=f"tayara_topic"
KAFKA_SERVER="kafka-broker:29092"

url = "https://www.tayara.tn/api/marketplace/search-api/"

property_categories={
    'À Louer':"louer",
    # 'À Vendre':'vendre'
}
property_types={
    '60be84be50ab95b45b08a09f':'bureau',
    # '60be84bd50ab95b45b08a09d':'villa',
    # '60be84bd50ab95b45b08a09c':'appartement',
    # '60be84be50ab95b45b08a0a0':'local-commercial',
    # '60be84be50ab95b45b08a0a1':'terrain',
}


headers = {
  'accept': 'application/json',
  'accept-language': 'en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7,ar;q=0.6',
  'content-type': 'application/json',
  'cookie': 'rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX18uZyFbSMQDoGuptOWRlEySJTZEympbxzM%3D; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX1845Ob5K%2FFK5uHYDXA6%2BtDCNxF4Qa1JIu4%3D; _gcl_au=1.1.430962370.1726233850; rs_ga=GA1.1.887514326.1726233851; caravel-cookie="5e53e2227ef8cc3b"; _gid=GA1.2.633884130.1726233934; _ga=GA1.1.887514326.1726233851; _ga_0LMQZK4Q75=GS1.1.1726257232.3.1.1726258721.60.0.0; rl_session=RudderEncrypt%3AU2FsdGVkX19ewqLmlDRLtjVDGyJ3iwLGBianSFeRRes2bv6J9QwNFl502CMUnD%2FoxXpBfrH4AeYU%2BC4xnMZrcxJa%2BJAvwqH2VzMHVNE4ahJYcvMrOxJ2ai3%2FZysrvezVnSjeepCAm0kLgqOCJgkTsg%3D%3D; rl_user_id=RudderEncrypt%3AU2FsdGVkX19BHEyDaj0xPylDPLyETNzgcQrMAEjhUgc%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX1%2FILIvWT8fVZAgJTTeCL1RoNq0y5eK%2F9EK%2FCxlQ0LiqaYyHa1rFhf5WwDS%2FBoIYmwpOhIaDRe7PJA%3D%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX19Vx3nBRAosNLHpQuc%2FJO6mm5OVe40SB1s%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX1%2B6oatwnagFQ65yglOH7B%2FF0hZ5F2DtobY%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX19vh6QLSMeXVrJes%2B5wn%2Bx8kObCN3HQEyY%3D; rs_ga_93M2KYSFK9=GS1.1.1726257228.3.1.1726258722.58.0.0',
  'origin': 'https://www.tayara.tn',
  'priority': 'u=1, i',
  'referer': 'https://www.tayara.tn/ads/c/Immobilier/Maisons%20et%20Villas/?Type+de+transaction=%C3%80+Vendre&page=2',
  'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}










# try:
#     response_json = json.loads(response.text)
#     print(len(response_json[0][0]))
#     print(len(response_json[1][0]))
#     with open ('/home/melek/RealStateDataEngineering/sample_response/tayara.json',"w", encoding='utf-8') as f :
#         json.dump(response_json,f, indent=2, ensure_ascii=False)

#       # Pretty print the JSON
# except json.JSONDecodeError:
    # print("Response content is not valid JSON.")

def tayara_get_data(property_type,property_category,producer):

  payload = json.dumps({
    "searchRequest": {
      "query": "",
      "offset": 0,
      "limit": 9999,
      "sort": 0,
      "filter": {
        "categoryId": "60be84bc50ab95b45b08a093",
        "subCategoryId": property_type,
        "adParamsMap": {
          "Type de transaction": property_category
        },
        "rangeAdParamsMap": {},
        "governorate": "",
        "delegation": [
          ""
        ],
        "minPrice": 0,
        "maxPrice": 0,
        "level": 0,
        "state": 2
      }
    }
  })
  
  try:
    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code != 200 :
       logger.error(f"Failed to get data from Tayara API. Status: {response.status}")
       return None 
    
    response_json = json.loads(response.text)
    data=response_json[0][0]
    



    # print(len(response_json[0][0]))
    # with open ('/home/melek/RealStateDataEngineering/sample_response/tayara.json',"w", encoding='utf-8') as f :
    #     json.dump(response_json[0][0],f, indent=2, ensure_ascii=False)
  
  except Exception as e:
    logger.error(
        f"Error occurred while sending request to Tayara API {property_type,property_category}: {e}"
    )
    return None


            
def tayara_all_data():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    for property_type in property_types:
        logger.info(f"starting scraping for type {property_types[property_type]}")
        for property_category in property_categories:
            tayara_get_data(property_type,property_category,producer)


tayara_all_data()