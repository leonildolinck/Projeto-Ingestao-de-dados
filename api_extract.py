import json
import config
import requests
from datetime import date

API = config.API_KEY
url_base = 'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/'


def obter_lista(data):
    resultado = requests.get(
        f'{url_base}{data}?adjusted=true&apiKey={API}'
    ).json()
    return resultado


data = date.today()
print(data)
resultado = obter_lista(data)

with open(f"preco_acoes_{data}.json", "w") as outfile:
    json.dump(resultado, outfile)


