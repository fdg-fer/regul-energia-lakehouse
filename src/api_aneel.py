#%%

# Converte para dataframe
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import logging


# --- logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_ckan.log"), logging.StreamHandler()]
)


load_dotenv()
ENGINE = create_engine(os.getenv("DB_URL"), future=True)

CKAN = "https://dadosabertos.aneel.gov.br/api/3/action"
# ID da tabela continuidades DEC FEC
RID_CONT = "4493985c-baea-429c-9df5-3030422c71d7"
# ID da tabela compensacoes 
RID_COMP = "364d945e-a18b-4111-ab1b-73aa0f7b06b1"
BATCH = 50000
TIMEOUT = 60
COL_DATA = "DatGeracaoConjuntoDados"  # nome da coluna de data
SCHEMA_STG = "stg" 


def _max_data_db(tabela: str):
    """Lê MAX(data) da tabela no Postgres; retorna pandas.Timestamp ou None."""
    try:
        with ENGINE.begin() as c:
            val = c.execute(text(f'SELECT MAX("{COL_DATA}") FROM {SCHEMA_STG}.{tabela}')).scalar()
    except Exception:
        return None
    return pd.to_datetime(val, errors="coerce") if val else None


def _max_data_api(resource_id: str, where_sql: str = ""):
    """Lê MAX(data) na API CKAN (via SQL)."""
    sql = f'SELECT MAX("{COL_DATA}") AS mx FROM "{resource_id}" {where_sql}'
    r = requests.get(f"{CKAN}/datastore_search_sql", params={"sql": sql}, timeout=TIMEOUT)
    r.raise_for_status()
    recs = r.json()["result"]["records"]
    mx = recs[0]["mx"] if recs else None
    return pd.to_datetime(mx, errors="coerce") if mx else None


def buscar_ckan_full(resource_id:str,filtros_dict, tabela:str):
    # 1) pega datas max no DB e na API (mesmo recorte de indicadores)
    db_max  = _max_data_db(tabela)
    where_i = 'WHERE "SigIndicador" IN (\'DEC\', \'FEC\')'
    api_max = _max_data_api(resource_id, where_i)


    if db_max is not None and api_max is not None and api_max <= db_max:
        logging.info(f'{tabela}: sem novidades (API MAX {api_max.date()} <= DB MAX {db_max.date()}).')
        return
    
    filtros = json.dumps(filtros_dict)
    offset, total = 0,0

    with ENGINE.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE IF EXISTS {SCHEMA_STG}.{tabela}')
        logging.info(f'DROP {SCHEMA_STG}.{tabela}')


    while True:
        r = requests.get(f"{CKAN}/datastore_search", 
                        params = {"resource_id":resource_id, "limit":BATCH, "offset":offset, "filters":filtros}, 
                        timeout=60)

        r.raise_for_status()
        rows = r.json()["result"].get("records", [])
        if not rows:
            break
        
        df = pd.DataFrame(rows)
        df["SigAgente"] = df["SigAgente"].astype(str).str.strip()
        df.to_sql(tabela, ENGINE, schema="stg", if_exists="append", index=False, method="multi", chunksize=50000)
        got = len(df)
        total += got
        offset += got
        logging.info(f"[{tabela}] +{got:,} (total {total:,})")
        
    logging.info(f"{tabela}: inseridas {total:,} linhas...")


def buscar_ckan_vigente(resource_id:str, filtros_dict, tabela:str, ano: int = 2025):
    # 1) pega datas max no DB e na API (mesmo recorte de indicadores)
    db_max  = _max_data_db(tabela)
    where_i = 'WHERE "SigIndicador" IN (\'DEC\', \'FEC\')'
    api_max = _max_data_api(resource_id, where_i)

    if db_max is not None and api_max is not None and api_max <= db_max:
        logging.info(f'{tabela}: sem novidades (API MAX {api_max.date()} <= DB MAX {db_max.date()}).')
        return

    # --- LIMPA SÓ O ANO-ALVO NO BANCO (em vez de DROPar a tabela) ---
    with ENGINE.begin() as conn:
        conn.execute(
            text(f'DELETE FROM {SCHEMA_STG}.{tabela} WHERE CAST("AnoIndice" AS INT) = :ano'),
            {"ano": int(ano)}
        )
        logging.info(f'{tabela}: DELETE ano={ano} (atualização ano vigente)')

    # --- adiciona AnoIndice=2025 no filtro da API, preservando seus filtros existentes ---
    fdict = dict(filtros_dict) if filtros_dict else {}
    # envia como string primeiro (cobre recurso onde AnoIndice é TEXT)
    fdict["AnoIndice"] = str(ano)
    filtros = json.dumps(fdict)

    offset, total = 0, 0

    while True:
        r = requests.get(
            f"{CKAN}/datastore_search",
            params={"resource_id": resource_id, "limit": BATCH, "offset": offset, "filters": filtros},
            timeout=60
        )
        r.raise_for_status()
        rows = r.json()["result"].get("records", [])
        if not rows:
            # fallback mínimo: se foi a primeira página e veio vazio, tenta ano como INT
            if offset == 0 and fdict.get("AnoIndice") == str(ano):
                fdict["AnoIndice"] = int(ano)
                filtros = json.dumps(fdict)
                # tenta de novo uma única vez
                r = requests.get(
                    f"{CKAN}/datastore_search",
                    params={"resource_id": resource_id, "limit": BATCH, "offset": 0, "filters": filtros},
                    timeout=60
                )
                r.raise_for_status()
                rows = r.json()["result"].get("records", [])
                if not rows:
                    break
                offset = 0  # reinicia paginação com o novo filtro
            else:
                break

        df = pd.DataFrame(rows)
        if "SigAgente" in df.columns:
            df["SigAgente"] = df["SigAgente"].astype(str).str.strip()

        df.to_sql(tabela, ENGINE, schema=SCHEMA_STG, if_exists="append", index=False, method="multi", chunksize=50000)

        got = len(df)
        total += got
        offset += got
        logging.info(f"[{tabela}] +{got:,} (total {total:,})")

    logging.info(f"{tabela}: inseridas {total:,} linhas para o ano {ano}.")


def load_cont_full():
        buscar_ckan_full(
        RID_CONT,
        {"SigIndicador": ["DEC", "FEC"]}, "stg_continuidades")
        
        
def load_comp_full():
        buscar_ckan_full(
        RID_COMP,
        {"SigIndicador": ['PGUCAT', 'PGUCBTNU', 'PGUCBTU', 'PGUCMTNU', 'PGUCMTU']}, "stg_compensacoes")       


def load_cont_vigente():
        buscar_ckan_vigente(
        RID_CONT,
        {"SigIndicador": ["DEC", "FEC"]}, "stg_continuidades", ano=2025)
        
        
def load_comp_vigente():
        buscar_ckan_vigente(
        RID_COMP,
        {"SigIndicador": ['PGUCAT', 'PGUCBTNU', 'PGUCBTU', 'PGUCMTNU', 'PGUCMTU']}, "stg_compensacoes", ano=2025)       