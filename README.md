# Pipeline de Dados Financeiros com Airflow e Snowflake

Pipeline de dados automatizada desenvolvida em **Apache Airflow** para extração de indicadores económicos e taxas de câmbio de APIs públicas, carga em tabelas temporárias no **Snowflake** e consolidação final através de Stored Procedures transacionais.

---

## 🛠️ Arquitetura e Tecnologias

* **Orquestração**: Apache Airflow (com TaskFlow API e TaskGroups)
* **Linguagem**: Python 3.8+ (com Pandas para tratamento de dados)
* **Data Warehouse**: Snowflake (`FINANCIAL` database, schema `DBO`)
* **Fontes de Dados**: 
  * **AwesomeAPI**: Cotações de moedas em tempo real (USD-BRL, EUR-BRL).
  * **Banco Central do Brasil (Bacen)**: Indicadores macroeconómicos e inflação (IPCA, SELIC, Meta SELIC).
* **Infraestrutura**: Docker & Docker Compose

---

## 📂 Estrutura do Projeto

```text
Financial_Data/
├── dags/
│   ├── dag_financial.py          # DAG principal de orquestração financeira
│   └── dag_procedure.py          # DAGs auxiliares ou de teste de procedures
├── analytics/                    # Notebooks de exploração e rascunhos de análise
├── logs/                         # Logs locais de execução do Airflow
├── plugins/                      # Plugins personalizados do Airflow
├── .dockerignore                 # Exclusões para o build da imagem Docker
├── .gitignore                    # Arquivos ignorados pelo controle de versão
└── docker-compose.yaml           # Configuração de containers do ambiente Airflow