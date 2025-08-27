# regul-energia-lakehouse

```text
[runjobs.py] ──chama──>  [Wrappers]
                             │
                             ├── load_continuidades() ──► baixar_e_carregar(READ_CONT, "stg_continuidades_2020_2025", filtros)
                             ├── load_compensacoes() ───► baixar_e_carregar(READ_COMP, "stg_compensacoes_2020_2025", filtros)
                             └── load_limites() ────────► baixar_e_carregar(READ_LIMIT, "stg_limites")

                                   │
                                   ▼
                           [Função CORE]
                        baixar_e_carregar(...)
      ┌────────────────────────────────────────────────────────────────┐
      │  1) Monta request CKAN (resource_id, limit, offset, filters)  │
      │  2) Faz paginação (while offset += batch)                     │
      │  3) Converte p/ DataFrame + limpeza básica (trim, tipos)      │
      │  4) Grava em Postgres (to_sql append, chunks)                 │
      └────────────────────────────────────────────────────────────────┘
                                   │
           ┌───────────────────────┴────────────────────────┐
           ▼                                                ▼
    [API CKAN / dados abertos]                       [PostgreSQL / Staging]
   (datastore_search / _sql)                        stg_continuidades_2020_2025
                                                    stg_compensacoes_2020_2025
                                                          stg_limites
```

## Estrutura do Repositório

```text
/docs/            # visão, diagramas, decisões de arquitetura
/src/
  ingestion/      # scripts de ingestão (CKAN -> staging no Postgres)
  quality/        # validações de data quality (ex: Pandera / Great Expectations)
  transforms/     # SQL: dimensões, fatos, views (camada core)
  analytics/      # notebooks e análises exploratórias
/app/             # app (ex: Streamlit) e guias de visualização (Power BI)
/infra/           # infraestrutura (docker-compose, configs, .env.example)
README.md         # visão geral do projeto
LICENSE           # licença do repositório

