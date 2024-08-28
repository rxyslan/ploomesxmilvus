import requests
import time
import datetime
import pytz
from datetime import datetime, timedelta
import pymongo

start_time = time.time()

ploomes_url = 'https://api2.ploomes.com/Contacts?$orderby=CreateDate+desc&$expand=Contacts'
ploomes_url_tasks = 'https://api2.ploomes.com/Tasks?$expand=Tags&$orderby=CreateDate+desc'
milvus_url_tickets = 'https://apiintegracao.milvus.com.br/api/chamado/criar'
ploomes_api_key = '5C6D7306E820C3A4F21B6F8F3F109B9E9D080C1F5AEA0027F3248641B87F0D3338739DA713441A88FE5527DAAA6A9C002E4A1CB52A9EFF74EBCC8F7ECF30F8F6'
headers_ploomes = {'user-key': ploomes_api_key}
milvus_url_criar = 'https://apiintegracao.milvus.com.br/api/cliente/criar'
milvus_url_buscar = 'https://apiintegracao.milvus.com.br/api/cliente/busca'
milvus_api_key = 'zV3fQrjuOSAjpiTkfEv6xIKkmB0bWWiF996ZPm67kFTT7joTpdVECJjhKJgvhj499z1REdc4pHCG0STbYc7Ntug2qyW4Xk6okJ4KO'
headers_milvus = {'Authorization': milvus_api_key}

response_ploomes = requests.get(ploomes_url, headers=headers_ploomes)

if response_ploomes.status_code == 200:
    data_ploomes = response_ploomes.json()
    clientes_ploomes = data_ploomes.get('value', [])
else:
    print("Erro ao obter info dos clientes:", response_ploomes.status_code)
    exit()

response_milvus = requests.get(milvus_url_buscar, headers=headers_milvus)

if response_milvus.status_code == 200:
    data_milvus = response_milvus.json()
    clientes_milvus = data_milvus.get('lista', [])
else:
    print("Erro ao obter info dos clientes:", response_milvus.status_code)
    exit()
    
# Conectar ao MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["PloomesxMilvus"]  # Substitua "meu_banco_de_dados" pelo nome do seu banco de dados
collection = db["collectionPloomesxMilvus"]  # Substitua "clientes_enviados" pelo nome da sua coleÃ§Ã£o

# Verificar se um cliente jÃ¡ foi enviado
def cliente_enviado(cnpj_cpf):
    return collection.find_one({"cnpj_cpf": cnpj_cpf}) is not None

# Marcar um cliente como enviado
def marcar_cliente_enviado(cnpj_cpf):
    collection.insert_one({"cnpj_cpf": cnpj_cpf})

existing_clients = set(c.get('cnpj_cpf') for c in clientes_milvus)
clients_to_send = []

for client_ploomes in clientes_ploomes:
    # Verifica se a data de criaÃ§Ã£o do cliente Ã© posterior a 24/02/2024
    create_date = client_ploomes.get('CreateDate')
    if create_date and create_date >= '2024-02-24':
        cnpj_cpf_cliente_ploomes = client_ploomes.get('CNPJ') or client_ploomes.get('CPF')

        if not cnpj_cpf_cliente_ploomes:  # Verifica se CPF ou CNPJ estÃ£o vazios
            print(f"Cliente '{client_ploomes.get('Name')}' possui CPF/CNPJ vazio. NÃ£o serÃ¡ enviado.")
            continue

        if cnpj_cpf_cliente_ploomes in existing_clients:
            print(f"Cliente com CNPJ/CPF '{cnpj_cpf_cliente_ploomes}' jÃ¡ existe no Milvus. NÃ£o serÃ¡ enviado novamente.")
        else:
            clients_to_send.append(client_ploomes)
            existing_clients.add(cnpj_cpf_cliente_ploomes)

for cliente_ploomes in clients_to_send:
    cnpj = cliente_ploomes.get('CNPJ')

    if cnpj and len(cnpj) > 11:
        tipo_pessoa = 1  # Pessoa jurÃ­dica
    else:
        tipo_pessoa = 2  # Pessoa fÃ­sica

    cliente_milvus = {
        "tipo_pessoa": tipo_pessoa,
        "cliente_documento": cliente_ploomes.get('CNPJ') or cliente_ploomes.get('CPF'),
        "cliente_site": cliente_ploomes.get('Website'),
        "cliente_observacao": cliente_ploomes.get('Note'),
        "cliente_ativo": True,
    }

    if tipo_pessoa == 2:  # Pessoa fÃ­sica
        cliente_milvus["cliente_pessoa_fisica"] = {
            "nome": cliente_ploomes.get('Name'),
            "data_nascimento": cliente_ploomes.get('Birthday'),
            "sexo": cliente_ploomes.get('Gender', '')  # Se 'Gender' nÃ£o estiver presente, deixe vazio
        }
    else:  # Pessoa jurÃ­dica
        cliente_milvus["cliente_pessoa_juridica"] = {
            "nome_fantasia": cliente_ploomes.get('LegalName') or cliente_ploomes.get('Name'),
            "razao_social": cliente_ploomes.get('LegalName') or cliente_ploomes.get('Name'),
            "inscricao_estadual": cliente_ploomes.get('StateRegistration', '')  # Se 'StateRegistration' nÃ£o estiver presente, deixe vazio
        }

    cliente_milvus["cliente_enderecos"] = [{
        "endereco_padrao": True,
        "endereco_descricao": cliente_ploomes.get('Note'),
        "endereco_cep": cliente_ploomes.get('ZipCode'),
        "endereco_logradouro": cliente_ploomes.get('StreetAddress'),
        "endereco_numero": cliente_ploomes.get('StreetAddressNumber'),
        "endereco_complemento": cliente_ploomes.get('StreetAddressLine2'),
        "endereco_bairro": cliente_ploomes.get('Neighborhood'),
        "endereco_cidade": cliente_ploomes.get('CityId'),
        "endereco_estado": cliente_ploomes.get('StateId')
    }]

    cliente_milvus["cliente_contatos"] = []

    for contato in cliente_ploomes.get('Contacts', []):
        contato_dict = {
            "contato_padrao": True,  # Defina como True se este for o contato padrÃ£o
            "contato_descricao": contato.get('Name', ''),  # DescriÃ§Ã£o do contato (vazio no exemplo)
            "contato_email": contato.get('Email', ''),  # Email do contato
            "contato_telefone": contato.get('Phone', ''),  # Telefone do contato (vazio se nÃ£o estiver presente)
            "contato_celular": contato.get('MobilePhone', ''),  # Celular do contato (vazio se nÃ£o estiver presente)
            "contato_observacao": contato.get('Note', '')  # ObservaÃ§Ã£o do contato (vazio no exemplo)
        }

        # Adiciona o dicionÃ¡rio do contato Ã  lista de contatos do cliente
        cliente_milvus["cliente_contatos"].append(contato_dict)

    tentativas = 0
    max_tentativas = 3

    while tentativas < max_tentativas:
        response_milvus = requests.post(milvus_url_criar, json=cliente_milvus, headers=headers_milvus)

        if response_milvus.status_code == 200:
            print(f"Cliente {cliente_ploomes.get('Name')} enviado para o Milvus com sucesso.")
            # Marcar o cliente como enviado no MongoDB
            marcar_cliente_enviado(cliente_milvus["cliente_documento"])
            break
        elif response_milvus.status_code == 429:
            tentativas += 1
            print(f"Tentativa {tentativas}: Aguardando antes de reenviar...")
            time.sleep(5)
        else:
            print(f"Erro ao enviar cliente {cliente_ploomes.get('Name')} para o Milvus:", response_milvus.status_code)
            print(response_milvus.text)
            break


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["PloomesxMilvus"]  # Substitua "meu_banco_de_dados" pelo nome do seu banco de dados
collection = db["collectionPloomesxMilvus"]  # Substitua "clientes_enviados" pelo nome da sua coleÃ§Ã£o

# Verificar se um cliente jÃ¡ foi enviado para o Ploomes
def cliente_enviado_ploomes(cnpj_cpf):
    return collection.find_one({"cnpj_cpf": cnpj_cpf}) is not None

# Marcar um cliente como enviado para o Ploomes
def marcar_cliente_enviado_ploomes(cnpj_cpf):
    collection.insert_one({"cnpj_cpf": cnpj_cpf})

existing_clients = set(c.get('CNPJ') or c.get('CPF') for c in clientes_ploomes)
clients_to_send = []

for client_milvus in clientes_milvus:
    cnpj_cpf_cliente_milvus = client_milvus.get('cnpj_cpf')

    if cliente_enviado_ploomes(cnpj_cpf_cliente_milvus):
        print(f"Cliente com CPF/CNPJ '{cnpj_cpf_cliente_milvus}' jÃ¡ existe no Ploomes. NÃ£o enviado novamente.")
    elif client_milvus.get('id') > 634501:
        existing_clients.add(cnpj_cpf_cliente_milvus)
        clients_to_send.append(client_milvus)

print(f"Total clients in Milvus: {len(clientes_milvus)}")
print(f"Total existing clients in Ploomes: {len(existing_clients)}")
print(f"Clients to send to Ploomes: {len(clients_to_send)}")

for cliente_milvus in clients_to_send:    
    tipo_pessoa = 1 if len(cliente_milvus['cnpj_cpf']) == 14 else 0  # Verifica se Ã© pessoa fÃ­sica ou jurÃ­dica
    print(f"Sending client to Ploomes: {cliente_milvus.get('Nome')}")

    
    cliente_ploomes = {
        "Name": cliente_milvus.get('nome_fantasia') or cliente_milvus.get('razao_social'),  # Ou "Name" dependendo do contexto
        "Neighborhood": cliente_milvus.get(""),
        "ZipCode": cliente_milvus.get(''),  # Supondo que seja um nÃºmero
        "Register": cliente_milvus.get('cnpj_cpf'),  # Verificar se 'cnpj_cpf' Ã© o campo correto
        "OriginId": cliente_milvus.get(''),  # Defina conforme necessÃ¡rio
        "StreetAddressNumber": cliente_milvus.get(""),
        # Adapte conforme necessÃ¡rio para outros campos

        # Exemplo de estrutura de telefone e propriedades adicionais
        "Phones": [
            {
                "PhoneNumber": "",
                "TypeId": 0,
                "CountryId": 0
            }
        ],
        "OtherProperties": [
            {
                "FieldKey": "{fieldKey}",
                "StringValue": "texto exemplo"
            },
            {
                "FieldKey": "{fieldKey}",
                "IntegerValue": 2
            }
        ]
    }

    tentativas = 0
    max_tentativas = 3  # Defina o nÃºmero mÃ¡ximo de tentativas conforme necessÃ¡rio

    while tentativas < max_tentativas:
        response_ploomes = requests.post(ploomes_url, json=cliente_ploomes, headers=headers_ploomes)

        if response_ploomes.status_code == 200:
            print(f"Cliente '{cliente_milvus.get('LegalName') or cliente_milvus.get('Name')}' enviado para o Ploomes com sucesso.")
            # Marcar o cliente como enviado no MongoDB
            marcar_cliente_enviado_ploomes(cliente_milvus.get('cnpj_cpf'))
            break
        elif response_ploomes.status_code == 429:
            tentativas += 1
            print(f"Tentativa {tentativas}: Aguardando antes de reenviar...")
            time.sleep(5)  # Aguarde 5 segundos entre as tentativas (ajuste conforme necessÃ¡rio)
        else:
            print(f"Erro ao enviar cliente '{cliente_milvus.get('LegalName') or cliente_milvus.get('Name')}' para o Ploomes:", response_ploomes.status_code)
            print(response_ploomes.text)
            break

response_ploomes = requests.get(ploomes_url_tasks, headers=headers_ploomes)

if response_ploomes.status_code == 200:
    data_ploomes = response_ploomes.json()
    tasks_data_ploomes = data_ploomes.get('value', [])
else:
    print("Erro ao obter info dos clientes:", response_ploomes.status_code)
    exit()

tasks_data_ploomes

import requests

todos_clientes_ploomes = []

endpoints = [
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=300",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=600",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=900",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=1200",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=1500",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=1800",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=2100",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=2400",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=2700",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=3000",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=3300",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=3600",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=3900",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=4200",
    "https://public-api2.ploomes.com/Contacts?$expand=Contacts&$skip=4500",
]

for endpoint in endpoints:
    response = requests.get(endpoint, headers=headers_ploomes)  # Suponha que 'headers' jÃ¡ tenha sido definido com os cabeÃ§alhos necessÃ¡rios
    if response.status_code == 200:
        todos_clientes_ploomes.extend(response.json().get('value', []))
    else:
        print(f"Erro ao fazer a requisiÃ§Ã£o para {endpoint}. CÃ³digo de status: {response.status_code}")

print("Clientes do Ploomes obtidos com sucesso!")



fuso_horario = pytz.timezone('America/Sao_Paulo')
hora_atual = datetime.now(tz=fuso_horario)


# Iterar sobre as tarefas do Ploomes
for task_data_ploomes in tasks_data_ploomes:
    # Verificar se a tarefa tem as tags necessÃ¡rias e foi criada dentro dos Ãºltimos 10 minutos
    if 'Tags' in task_data_ploomes and any(tag.get('TagId') in [40049715, 40049716, 40054183] for tag in task_data_ploomes['Tags']):
        data_hora_str = task_data_ploomes['CreateDate']
        data_hora_str = data_hora_str[:19]
        data_hora_tarefa = datetime.strptime(data_hora_str, '%Y-%m-%dT%H:%M:%S')
        data_hora_tarefa = fuso_horario.localize(data_hora_tarefa)
        diferenca_tempo = hora_atual - data_hora_tarefa
        diferenca_minutos = diferenca_tempo.total_seconds() / 60
        
        if diferenca_minutos <= 5:
            # Iterar sobre os clientes do Ploomes
            for cliente_ploomes in todos_clientes_ploomes:
                cpf_cnpj_ploomes = cliente_ploomes.get('CPF') or cliente_ploomes.get('CNPJ')
                
                if cpf_cnpj_ploomes:
                    cliente_id_ploomes = cliente_ploomes.get('Id')

                    # Encontrar o cliente correspondente no sistema Milvus
                    for cliente_milvus in clientes_milvus:
                        cnpj_cpf_milvus = cliente_milvus.get('cnpj_cpf')
                        
                        if cpf_cnpj_ploomes == cnpj_cpf_milvus:
                            cliente_id_milvus = cliente_milvus['token']
                            
                            # Verificar se o ContactId da tarefa coincide com o Id do cliente do Ploomes
                            if task_data_ploomes.get('ContactId') == cliente_id_ploomes:
                                # Enviar a tarefa para o Milvus apenas se os CPF/CNPJ coincidirem e o ContactId da tarefa coincidir com o Id do cliente do Ploomes
                                milvus_ticket_data = {
                                    "cliente_id": str(cliente_id_milvus),
                                    "chamado_assunto": task_data_ploomes.get("Title"),
                                    "chamado_descricao": task_data_ploomes.get("Description"),
                                    "chamado_email": "",
                                    "chamado_telefone": "",  
                                    "chamado_contato": "", 
                                    "chamado_tecnico": "",  
                                    "chamado_mesa": "",  
                                    "chamado_setor": "",  
                                    "chamado_categoria_primaria": "ManutenÃ§Ã£o" if any(tag.get('TagId') == 40054183 for tag in task_data_ploomes['Tags']) else "InstalaÃ§Ã£o" if any(tag.get('TagId') == 40049715 for tag in task_data_ploomes['Tags']) else "Entrega", 
                                    "chamado_categoria_secundaria": ""  
                                }

                                response_milvus = requests.post(milvus_url_tickets, json=milvus_ticket_data, headers=headers_milvus)
                                
                                if response_milvus.status_code == 200:
                                    print("Tarefa enviada com sucesso para o Milvus!")
                                    break  # Parar de iterar sobre os clientes do Milvus apÃ³s enviar a tarefa
                                else:
                                    print(f"Erro ao enviar a tarefa para o Milvus. CÃ³digo de status: {response_milvus.status_code}")
                                    print("Resposta do servidor Milvus:", response_milvus.text)
                            else:
                                print("O ContactId da tarefa nÃ£o coincide com o Id do cliente do Ploomes.")
                                break  # Parar de iterar sobre os clientes do Milvus se o ContactId nÃ£o coincidir
                        else:
                            print("CPF/CNPJ do cliente no Ploomes nÃ£o coincide com nenhum cliente no Milvus.")


end_time = time.time()

execution_time = end_time - start_time

print(f"Tempo de execuÃ§Ã£o: {execution_time} segundos")
