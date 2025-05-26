
from dagster import asset, Definitions, Config, EnvVar
import requests
from typing import Dict, Any, Optional
import json
from datetime import datetime

class APIConfig(Config):
    """Configurações dinâmicas para a chamada de API"""
    url: str = "https://jsonplaceholder.typicode.com/posts/1"
    timeout: int = 30  # segundos
    method: str = "GET"
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None

@asset(name="chamada_api_teste")
def api_call_asset(context, config: APIConfig) -> Dict[str, Any]:
    """
    Asset que realiza chamadas HTTP configuráveis e retorna a resposta.
    
    Args:
        config: Contém URL, método HTTP, timeout e outros parâmetros
    
    Returns:
        Dict com a resposta da API e metadados
    """
    # Log inicial com configurações
    context.log.info(f"🔵 Iniciando chamada para: {config.url}")
    # context.log.debug(f"Configurações: {config.model_dump()}")
    context.log.info(f"Configurações: {config.model_dump()}")

    # Registra o tempo de início
    start_time = datetime.now()
    
    try:
        # Faz a requisição HTTP
        response = requests.request(
            method=config.method,
            url=config.url,
            params=config.params,
            headers=config.headers,
            timeout=config.timeout
        )
        duration = (datetime.now() - start_time).total_seconds()
        
        # Verifica status code
        if not response.ok:
            context.log.error(f"🔴 Erro na API (HTTP {response.status_code}): {response.text}")
            response.raise_for_status()
        
        # Processa a resposta
        data = response.json()
        
        # Log de sucesso
        context.log.info(f"🟢 Sucesso! Status: {response.status_code}")
        context.log.info(f"Tempo de resposta: {duration:.2f}s")
        # context.log.debug(f"Resposta completa: {json.dumps(data, indent=2)}")
        context.log.info(f"Resposta completa: {json.dumps(data, indent=2)}")
        
        return {
            "metadata": {
                "status_code": response.status_code,
                "request_time": duration,
                "timestamp": start_time.isoformat(),
                "url": config.url
            },
            "data": data
        }
        
    except requests.Timeout:
        context.log.error("🟠 Timeout: A API não respondeu dentro do tempo limite")
        raise
    except requests.RequestException as e:
        context.log.error(f"🔴 Falha na requisição: {str(e)}")
        raise
    except json.JSONDecodeError:
        context.log.error("🔴 Não foi possível decodificar a resposta JSON")
        raise
    except Exception as e:
        context.log.error(f"🔴 Erro inesperado: {str(e)}")
        raise

defs = Definitions(
    assets=[api_call_asset],
    # Configurações padrão podem ser injetadas via env vars
    # Exemplo:
    # resources={
    #     "api_config": {
    #         "url": EnvVar("API_BASE_URL"),
    #         "headers": {
    #             "Authorization": EnvVar("API_TOKEN")
    #         }
    #     }
    # }
)