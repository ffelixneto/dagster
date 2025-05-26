import os
import time
import json
import requests

import polars as pl

# Importações do Dagster
from dagster import (
    asset,
    Definitions,
    Config,
    io_manager,
    IOManager,
    AssetMaterialization,
    MetadataValue,
    RetryPolicy,
    sensor, RunRequest, SensorResult
)


# Importações adicionais
from dagster import ScheduleDefinition, define_asset_job, AssetSelection


from datetime import datetime
from typing import Dict, Any, Optional


# Config da API
class APIConfig(Config):
    """Configurações dinâmicas para a chamada de API"""
    url: str = "https://jsonplaceholder.typicode.com/posts/1"
    timeout: int = 30  # segundos
    method: str = "GET"
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None


# Implementação do IOManager direto no arquivo
class LocalFileIOManager(IOManager):
    def _get_path(self, asset_key: str) -> str:
        os.makedirs("/tmp/dagster_outputs", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"/tmp/dagster_outputs/{asset_key}_{timestamp}"

    def handle_output(self, context, obj: Any):
        path_prefix = self._get_path(context.asset_key.path[-1])
        
        if isinstance(obj, pl.DataFrame):
            # Caso especial para DataFrames Polars
            parquet_path = f"{path_prefix}.parquet"
            obj.write_parquet(parquet_path)

            context.add_output_metadata({
                "path": MetadataValue.path(parquet_path),
                "rows": MetadataValue.int(obj.height),
                "columns": MetadataValue.int(obj.width),
                "size": MetadataValue.int(os.path.getsize(parquet_path))
            })

            # yield AssetMaterialization(
            #     asset_key=context.asset_key,
            #     description="DataFrame salvo como Parquet",
            #     metadata={
            #         "path": MetadataValue.path(parquet_path),
            #         "rows": MetadataValue.int(obj.height),
            #         "columns": MetadataValue.int(obj.width),
            #         "size": MetadataValue.int(os.path.getsize(parquet_path))
            #     }
            # )
            # return None
        else:
            # Mantém o comportamento original para outros tipos
            json_path = f"{path_prefix}.json"
            with open(json_path, "w") as f:
                json.dump(obj, f, indent=2)
            
            context.add_output_metadata({
                "path": MetadataValue.path(json_path),
                "size": MetadataValue.int(os.path.getsize(json_path))
            })

            # yield AssetMaterialization(
            #     asset_key=context.asset_key,
            #     description="Dados persistidos localmente",
            #     metadata={
            #         "path": MetadataValue.path(json_path),
            #         "size": MetadataValue.int(os.path.getsize(json_path))
            #     }
            # )
            # return None

    def load_input(self, context):
        asset_key = context.asset_key.path[-1]
        files = [f for f in os.listdir("/tmp/dagster_outputs") if f.startswith(asset_key)]
        
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado para {asset_key}")
            
        latest = max(files, key=lambda f: os.path.getctime(os.path.join("/tmp/dagster_outputs", f)))
        file_path = os.path.join("/tmp/dagster_outputs", latest)
        
        if file_path.endswith('.parquet'):
            return pl.read_parquet(file_path)
        else:
            with open(file_path, "r") as f:
                return json.load(f)

@io_manager
def local_file_io_manager(_):
    return LocalFileIOManager()

# =============================================================================
# ASSETS 
# =============================================================================

# Asset principal
@asset(name="chamada_api_teste", retry_policy=RetryPolicy(max_retries=3, delay=5))
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


@asset(
    name="api_data_parquet",
    retry_policy=RetryPolicy(max_retries=3, delay=5)
)
def api_data_to_parquet(context, chamada_api_teste: Dict[str, Any]):
    """
    Transforma os dados da API em DataFrame Polars.
    O IOManager customizado agora cuida de salvar como Parquet.
    """
    try:
        api_data = chamada_api_teste["data"]
        
        df = pl.DataFrame({
            "user_id": [api_data["userId"]],
            "post_id": [api_data["id"]],
            "title": [api_data["title"]],
            "body": [api_data["body"]],
            "load_timestamp": [datetime.now().isoformat()]
        })
        
        context.log.info(f"DataFrame criado com {df.height} linhas")
        return df  # Apenas retorne o DataFrame, o IOManager cuida do resto
        
    except Exception as e:
        context.log.error(f"Erro ao criar DataFrame: {str(e)}")
        raise



# DEFINIÇÃO DO JOB (FLUXO)
# Cria um job que inclui ambos os assets em sequência
api_to_parquet_job = define_asset_job(
    name="api_to_parquet_job",
    selection=AssetSelection.assets(api_call_asset, api_data_to_parquet)
    # selection=AssetSelection.all()  # Ou selecione específicos: AssetSelection.assets(api_call_asset, api_data_to_parquet)
)

# DEFINIÇÃO DO AGENDAMENTO
# Agenda para executar diariamente às 9AM
api_schedule = ScheduleDefinition(
    job=api_to_parquet_job,
    cron_schedule="0 9 * * *",  # Todos os dias às 9:00
    execution_timezone="America/Sao_Paulo"  # Ajuste para seu fuso
)


@sensor(
    job=api_to_parquet_job,
    minimum_interval_seconds=3600
)
def new_json_file_sensor(context):
    # Pasta para monitorar
    watch_dir = "/tmp/dagster_inputs/"
    os.makedirs(watch_dir, exist_ok=True)

    # Lista arquivos JSON recentes (últimas 24h)
    files = [
        f for f in os.listdir(watch_dir) 
        if f.endswith(".json") 
        and (time.time() - os.path.getmtime(os.path.join(watch_dir, f))) < 86400
    ]

    if files:
        # Dispara o job para cada arquivo novo
        return SensorResult(
            run_requests=[RunRequest(run_key=f) for f in files]
        )

# Definições do Dagster
defs = Definitions(
    assets=[api_call_asset, api_data_to_parquet],
    jobs=[api_to_parquet_job],
    schedules=[api_schedule],
    sensors=[new_json_file_sensor],  # Adicione o sensor aqui
    resources={"io_manager": local_file_io_manager}
)
