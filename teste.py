url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2709-13-2026%27&@dataFinalCotacao=%2709-20-2026%27&$select=dataHoraCotacao,cotacaoCompra&$format=json"
print(url)

import requests

try:
    # fazendo o get na api
    response = requests.get(url, timeout=20)
    print(response.status_code)
    response.raise_for_status()
    print(type(response))
    # convertendo para json
    response = response.json()
    
except :
    print("deu erro")
    with open(f"landing/cotacao_dolar.json", "r") as file:
        import json
        response = json.load(file)



# buscando apenas a lista dos valores
registers = response["value"]
print(registers)
print(len(registers))
print(registers[0])