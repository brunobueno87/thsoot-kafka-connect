import requests
import json

"""Script que vc seta qual o worker-id que você quer encontrar todos os connectors que estão rodando nele.
   Estou partindo do principio que você esta rodando o cluster k8s rodando na porta 8083 fique atendo das descrições 
   do connector do seu cluster. Isso facilita a identificar baseado em metricas de GC da JVM, qual é o connector que esta
   sendo o ofensor do cluster.
"""

# Configuração da URL da API do Kafka Connect
KAFKA_CONNECT_URL = "https://end-point--kafka-connect.com"

# Função para obter a lista de conectores
def get_connectors():
    response = requests.get(f"{KAFKA_CONNECT_URL}/connectors", verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter conectores: {response.status_code}")
        return []

# Função para obter o status de um conector específico
def get_connector_status(connector_name):
    response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter status do conector {connector_name}: {response.status_code}")
        return None

# Função para listar conectores que estão rodando em um worker_id específico
def list_connectors_in_worker(worker_id):
    connectors = get_connectors()
    matching_connectors = []

    for connector in connectors:
        status = get_connector_status(connector)
        if status:
            # Verificar se o worker_id corresponde ao desejado
            current_worker_id = status.get('connector', {}).get('worker_id', '')
            if current_worker_id == worker_id:
                matching_connectors.append(connector)

    # Imprimir a lista de conectores correspondentes
    if matching_connectors:
        print(f"Conectores rodando no worker {worker_id}:")
        for connector in matching_connectors:
            print(f"- {connector}")
    else:
        print(f"Nenhum conector encontrado no worker {worker_id}")

# Exemplo de uso - forneça o worker_id que deseja verificar
if __name__ == "__main__":
    # Defina o worker_id que deseja buscar
    WORKER_ID = "seu.ip.aqui:8083"
    list_connectors_in_worker(WORKER_ID)

