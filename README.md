# Pipeline de Dados Financeiros com Airflow e Snowflake

Pipeline de dados automatizado desenvolvido em **Apache Airflow** para extração de indicadores econômicos e taxas de câmbio de APIs públicas, carga em tabelas de staging no **Snowflake** e consolidação final através de Stored Procedures transacionais.

## 🛠️ Arquitetura e Tecnologias

* **Orquestração**: Apache Airflow (com TaskFlow API e TaskGroups)
* **Linguagem**: Python 3.8+ (com Pandas para tratamento de dados)
* **Data Warehouse**: Snowflake (`FINANCIAL` database, schema `DBO`)[cite: 2]
* **Fontes de Dados**: 
  * **AwesomeAPI**: Cotações de moedas em tempo real (USD-BRL, EUR-BRL).
  * **Banco Central do Brasil (Bacen)**: Indicadores macroeconômicos e inflação (IPCA, SELIC, Meta SELIC).
* **Infraestrutura**: Docker & Docker Compose[cite: 5]

## 📂 Estrutura do Projeto

```text
Financial_Data/
├── dags/
│   ├── dag_financial.py          # DAG principal de orquestração financeira
│   └── dag_procedure.py          # DAG auxiliar para testes de procedures
├── plugins/                      # Plugins personalizados do Airflow
├── .dockerignore                 # Exclusões para o build da imagem Docker
├── .gitignore                    # Arquivos ignorados pelo controle de versão
├── docker-compose.yaml           # Configuração de containers do ambiente Airflow
└── README.md                     # Documentação do projeto
```

## 🔄 Fluxo do Pipeline (`dag_financial`)

1. **Extração (Requests)**: Consome os endpoints de câmbio da AwesomeAPI e os dados oficiais das séries temporais do Banco Central do Brasil (Bacen).
2. **Transformação e Limpeza (Pandas)**: Padroniza formatos de data, filtra colunas e converte os payloads JSON em DataFrames estruturados.
3. **Carga Staging (Snowflake)**: Insere os dados limpos nas tabelas temporárias (`EXANGE_RATE_TEMP` e `FINANCIAL_INDICATOR_TEMP`) utilizando o `SnowflakeHook`[cite: 2].
4. **Consolidação (Merge Procedures)**: Dispara Stored Procedures no Snowflake (`sp_process_exchange` e `sp_process_indicator`) para realizar o `MERGE` nas tabelas finais (`EXANGE_RATE` e `FINANCIAL_INDICATOR`) e limpar as tabelas de staging[cite: 2, 4].

## ❄️ Scripts do Snowflake

Execute os scripts abaixo no Snowflake para configurar o banco de dados, as tabelas e as procedures antes de rodar a DAG.

### 1. Criação das Tabelas (DDL)

```sql
USE DATABASE FINANCIAL;
USE SCHEMA DBO;

-- Tabelas de Câmbio
CREATE OR REPLACE TABLE EXANGE_RATE (
    Code VARCHAR,
    CodeIn VARCHAR,
    Name VARCHAR,
    High VARCHAR,
    Low VARCHAR,
    Bid VARCHAR,
    Ask VARCHAR,
    create_date TIMESTAMP
);

CREATE OR REPLACE TABLE EXANGE_RATE_TEMP (
    Code VARCHAR,
    CodeIn VARCHAR,
    Name VARCHAR,
    High VARCHAR,
    Low VARCHAR,
    Bid VARCHAR,
    Ask VARCHAR,
    create_date TIMESTAMP
);

-- Tabelas de Indicadores Financeiros
CREATE OR REPLACE TABLE FINANCIAL_INDICATOR (
    Indicator VARCHAR,
    Date_Visit DATE,
    Value_Indicator FLOAT
);

CREATE OR REPLACE TABLE FINANCIAL_INDICATOR_TEMP (
    Date_Visit DATE,
    Value_Indicator FLOAT,
    Indicator VARCHAR
);
```

### 2. Stored Procedures de Merge

```sql
use financial;

create or replace procedure sp_process_indicator()
returns varchar
language sql
as 
$$
begin

    begin transaction;

    merge into Financial_Indicator sink
    using Financial_Indicator_Temp source
    on source.indicator = sink.indicator
    and source.value_indicator = sink.value_indicator
    and source.date_visit = sink.date_visit
    
    when not matched then 
    insert (Indicator, Date_Visit, Value_Indicator)
    values(source.indicator,source.date_visit,source.value_indicator);

    truncate table Financial_Indicator_Temp;
    
    commit;
    
    return 'merge completed sucessfully';
    
end;
$$;

create or replace procedure sp_process_exchange()
returns varchar
language sql
as 
$$
begin

    begin transaction;
    
    merge into Exange_Rate sink
    using Exange_Rate_Temp source
    on source.name = sink.name
    and source.code = sink.code
    and source.bid = sink.bid
    and source.ask = sink.ask

    when not matched then 
    insert (Code, CodeIn, Name, High, Low, Bid, Ask, create_date)
    values (source.Code, source.CodeIn, source.Name, source.High, source.Low, source.Bid, source.Ask, source.create_date);

    truncate table Exange_Rate_Temp;
    
    commit;

    return 'merge completed sucessful';
    
end;
$$;
```

## 🚀 Como Executar o Projeto

### 1. Subir o Ambiente com Docker
Certifique-se de ter o **Docker** e o **Docker Compose** instalados. Na pasta raiz do projeto, inicie os containers:

```bash
docker compose up -d
```

### 2. Configurar as Conexões no Airflow
Acesse `http://localhost:8080` e crie as seguintes conexões em **Admin > Connections**:
* **`exchange-rate`**: HTTP Hook apontando para a URL base da AwesomeAPI.
* **`financial-ratio-index`**: HTTP Hook apontando para a URL de séries temporais do Bacen.
* **`snowflake-financial`**: Conexão do tipo *Snowflake*, configurada com sua conta, warehouse, database (`Financial`) e schema (`DBO`)[cite: 2, 3].

### 3. Executar a Pipeline
Ative a DAG **`dag_financial_data`** no painel do Airflow e dispare uma execução manual (**Trigger DAG**).

## 💡 Convenção de Commits

Este repositório segue o padrão **Conventional Commits**:
* `feat:` Nova funcionalidade.
* `fix:` Correção de bugs.
* `refactor:` Refatoração de código sem alteração de regras de negócio.
* `chore:` Atualizações de configuração, Docker ou dependências.
* `docs:` Alterações na documentação.