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

## Passos
1. **Banco**: criar DB `case_equatorial` e schemas `raw`, `stg`, `core`.
2. **Ingestão**: rodar scripts em `/src/ingestion/` (CKAN → `stg_*`).
3. **Transform**: `dbt init`, configurar profile Postgres, `dbt deps`, `dbt run`, `dbt test`.
4. **Observabilidade**: `edr report` (Elementary) para gerar relatório HTML de saúde.
5. **(Opcional)**: Painel Streamlit para métricas de qualidade (freshness, volumes, falhas).

## Qualidade & Observabilidade (o que é checado)
- **Conformidade**: tipos/valores válidos (`indicador ∈ {DEC,FEC}`, `mes ∈ 1..12`, `ano ∈ 2020..2025`)
- **Completude**: % nulos em campos críticos; meses faltantes por distribuidora
- **Consistência**: chaves únicas `(ide_conjunto, ano, mes, indicador)`; FK para `dim_conjunto`
- **Acurácia (pragmática)**: faixas plausíveis (FEC ≤ 50; DEC ≥ 0)
- **Pontualidade (Freshness)**: `MAX(dat_geracao)` dentro do SLA mensal
- **Volume**: linhas por mês comparado ao histórico

## Comandos úteis
```bash
# instalar pacotes
pip install -U pandas requests sqlalchemy psycopg2-binary python-dotenv dbt-postgres elementary-data

# rodar dbt
dbt deps
dbt run
dbt test

# relatório elementary
edr report

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

