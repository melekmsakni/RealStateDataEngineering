import requests
import json
import time
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tayara_API2.log"),  # Log to a file
        logging.StreamHandler()            # Also log to console
    ]
)

logger = logging.getLogger(__name__)

payload = {}
headers = {
  'accept': '*/*',
  'accept-language': 'en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7,ar;q=0.6',
  'cookie': 'caravel-cookie="d7bd221e5e4289dc"; _gcl_au=1.1.1486801219.1727114978; load-heavy-content=true; rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX19mExdMXxw0rHQD%2BxnFft0YAHSlUTCztMllkwYcG%2BhuAXA2VwunqKIs; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX1%2FF6oTwlXGSuyupLQRoSVw%2F5OZa4%2FDdpBoSCnXfc9tRUtZauWsWCnGD; rs_ga=GA1.1.1858177586.1727114984; caravel-cookie="902a9009ddae8717"; _gid=GA1.2.1463288821.1727130010; _gat_UA-190796392-1=1; _ga=GA1.1.1858177586.1727114984; rl_session=RudderEncrypt%3AU2FsdGVkX19GKtciLteDK%2BFA5HAczHvp7Nvu6yEjDVqxnntB1IFoK8AAdPYneY75Tw2r9JMulY7m0Wm%2BA8hgj96Tltthn8YlvE1rMHX6JGGZE1lIB8aRK%2BNlE8XIeb01%2FHhf3Xsbc3srY9QUmLCiTw%3D%3D; rl_user_id=RudderEncrypt%3AU2FsdGVkX1%2FDbiqmQHTrgg5d83ZkT%2FYh4A5lVuUsOFI%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX19FihPaSoJJWvP8SW6MMEewiSrcus6lTZKecLzsmH4QDjWSDXb0a5yOzTFJnsbvP%2BupHYEu4F7MzQ%3D%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX1%2B2OKh8WAcrvRCudNXBBjN8VIjeDFWnNTE%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX1%2FQdrv4tqr%2B7gPZrvnIfxAhWIbztLtbxPs%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX19RYrqoQ%2FYT22lZ4mxCitI%2FHw61yuq%2FG88%3D; _ga_0LMQZK4Q75=GS1.1.1727172039.3.1.1727172156.9.0.0; rs_ga_93M2KYSFK9=GS1.1.1727172032.3.1.1727172156.1.0.0',
  'priority': 'u=1, i',
  'referer': 'https://www.tayara.tn/ads/c/Immobilier/?page=1',
  'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'no-cors',
  'sec-fetch-site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}

with open ('/home/melek/RealStateDataEngineering/sample_response/tayara.json','r') as f :
    data=json.load(f)



for index,value in enumerate(data) :
    id=value['id']
    url = f"https://www.tayara.tn/item/{id}"
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200 :
        print(f'{index} succeess')

        with open('tayara_post.html','w')as f:
            f.write(response.text)
            break
    else :
        logging.error(f'{index} error - {id}  - {response.status_code}')


