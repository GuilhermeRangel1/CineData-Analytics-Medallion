# Engenharia de Dados | Visagio Rocket Lab 2026.2

## CineData Analytics: Pipeline de Filmes no Databricks

Projeto de Engenharia de Dados para ingestão, tratamento, modelagem dimensional e análise de dados cinematográficos. A solução foi construída no Databricks com PySpark e Spark SQL, seguindo a Arquitetura Medallion e utilizando um Workflow para orquestrar as etapas do pipeline.

## Sobre o projeto

O CineData Analytics integra informações de filmes provenientes de arquivos CSV com cotações de dólar obtidas pela API PTAX do Banco Central. O pipeline transforma dados brutos em tabelas analíticas prontas para responder perguntas de negócio e em uma tabela de contexto para aplicações de GenAI/RAG.

As camadas do Lakehouse são organizadas da seguinte forma:

- **Bronze:** preserva os dados de entrada e registra a ingestão.
- **Silver:** limpa, tipa, padroniza e organiza os dados em português.
- **Gold:** disponibiliza dimensões, fato, tabelas-ponte, análises de negócio e contexto para GenAI.

## Entregáveis

- Três notebooks PySpark em [`notebooks/`](notebooks/).
- Workflow Databricks com execução sequencial Bronze → Silver → Gold.
- Configuração exportada do Job em [`configuracao/databricks/job.yaml`](configuracao/databricks/job.yaml).
- Evidências da configuração e da execução bem-sucedida em [`evidencias/workflow/`](evidencias/workflow/).
- Modelo dimensional e consultas analíticas implementados na camada Gold.

## Arquitetura do pipeline

```text
┌───────────────────────────────────────────────────────┐
│                    FONTES DE DADOS                    │
│ CSVs de filmes, métricas, finanças e avaliações       │
│ Créditos e API PTAX do Banco Central                  │
└──────────────────────────┬────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────┐
│                    CAMADA BRONZE                      │
│ Dados brutos em tabelas Delta                         │
│ Controle de ingestão e rastreabilidade                │
└──────────────────────────┬────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────┐
│                    CAMADA SILVER                      │
│ Tipagem, limpeza, padronização e deduplicação         │
│ Regras de negócio para filmes e entidades             │
└──────────────────────────┬────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────┐
│                     CAMADA GOLD                       │
│ Modelo dimensional de filmes                          │
│ Fato, dimensões, tabelas-ponte e contexto GenAI       │
└───────────────────────────────────────────────────────┘
```

## Fontes de dados

Os arquivos de entrada são disponibilizados no Volume do Databricks configurado no notebook Bronze. As fontes utilizadas são:

| Fonte | Conteúdo |
|---|---|
| `movies_info_TMDB_IMDB.csv` | Informações gerais dos filmes, títulos, datas, status e sinopses |
| `movies_financials_IMDB_TMDB.csv` | Orçamentos, receitas e informações financeiras |
| `movies_metrics_IMDB_TMDB.csv` | Popularidade, notas e métricas de engajamento |
| `movies_reviews.csv` | Avaliações e notas de usuários |
| `credits_and_tags_IMDB_TMDB.csv` | Gêneros, pessoas, empresas e créditos associados |
| API PTAX do Banco Central | Cotação de compra do dólar por data |

O caminho padrão de entrada utilizado pelo notebook de ingestão é:

```text
/Volumes/workspace/default/inputs
```

## Notebooks

### 1. Landing to Bronze

Arquivo: [`01_landing_to_bronze.ipynb`](notebooks/01_landing_to_bronze.ipynb)

Responsável por:

1. Criar o schema `bronze` quando necessário.
2. Ler os arquivos CSV do Volume de entrada.
3. Persistir os dados brutos em tabelas Delta.
4. Adicionar informações de ingestão para rastreabilidade.
5. Consultar a API PTAX do Banco Central usando um intervalo de datas parametrizado.
6. Gravar a cotação em `bronze.tb_cotacao_dolar`.
7. Exibir validações da ingestão com `display()`.

O notebook utiliza widgets para configurar o caminho dos arquivos e as datas de início e fim da consulta da API.

### 2. Bronze to Silver

Arquivo: [`02_bronze_to_silver.ipynb`](notebooks/02_bronze_to_silver.ipynb)

Responsável pela preparação dos dados para consumo analítico. Entre os tratamentos realizados estão:

- conversão explícita de tipos;
- padronização dos nomes das colunas;
- tratamento de valores nulos e inconsistentes;
- deduplicação e validação de registros;
- limpeza de listas antes de operações com `explode()`;
- normalização das entidades de gêneros, pessoas e empresas;
- integração dos valores financeiros com a cotação do dólar;
- gravação das tabelas Silver em modo idempotente;
- validações com `display()` após cada conjunto de transformações.

### 3. Silver to Gold

Arquivo: [`03_silver_to_gold.ipynb`](notebooks/03_silver_to_gold.ipynb)

Responsável pela construção do modelo analítico e das respostas de negócio. O notebook cria:

#### Dimensões

- `gold.dim_movies`
- `gold.dim_genres`
- `gold.dim_people`
- `gold.dim_companies`
- `gold.dim_reviews`

#### Fato

- `gold.fact_movies_performance`

#### Tabelas-ponte

- `gold.bridge_movie_genre`
- `gold.bridge_movie_person`
- `gold.bridge_movie_company`

#### Contexto para GenAI

- `gold.gold_genai_movies_context`

As tabelas utilizam chaves substitutas e relações por identificadores de filme, permitindo análises por gênero, pessoa, produtora, avaliação, receita e métricas de engajamento.

## Análises de negócio

O notebook Gold responde às principais perguntas analíticas do projeto:

1. Receita total dos filmes em reais.
2. Cinco filmes com maior popularidade.
3. Quantidade de filmes por gênero.
4. Dez filmes com maior receita em dólares e reais, utilizando `RANK()`.
5. Ator com maior participação em filmes nos dois anos mais recentes da base.
6. Produtora com maior lucro nos cinco anos mais recentes da base.

Os resultados são apresentados diretamente no Databricks por meio de `display()`.

## Workflow Databricks

O pipeline é orquestrado pelo Job `CineData_Analytics_Medallion_Workflow`, com as tarefas sequenciais:

```text
to_Bronze  →  to_Silver  →  to_Gold
```

O Job possui agendamento diário no fuso `America/Sao_Paulo`. Também é possível executar o pipeline manualmente usando **Run now**.

A configuração exportada está em [`configuracao/databricks/job.yaml`](configuracao/databricks/job.yaml).

As evidências visuais estão disponíveis em:

- [`workflow-tasks.png`](evidencias/workflow/workflow-tasks.png) — nome do Job, grafo de dependências e agendamento.
- [`successful-run.png`](evidencias/workflow/successful-run.png) — execução concluída com sucesso e tarefas verdes.

## Como executar

### Pré-requisitos

- Workspace Databricks com permissão para criar notebooks, tabelas e Jobs.
- Compute Serverless ou cluster com suporte a PySpark.
- Arquivos CSV carregados no Volume configurado.
- Acesso de rede para consultar a API PTAX do Banco Central.

### Execução manual dos notebooks

1. Importe os notebooks da pasta `notebooks/` para o Workspace Databricks.
2. Confirme se os arquivos estão disponíveis em `/Volumes/workspace/default/inputs` ou ajuste o widget `input_base_path`.
3. Execute `01_landing_to_bronze.ipynb`.
4. Execute `02_bronze_to_silver.ipynb` após a conclusão da Bronze.
5. Execute `03_silver_to_gold.ipynb` após a conclusão da Silver.
6. Verifique as tabelas e os resultados exibidos nas células de validação.

### Execução pelo Workflow

1. Crie um Job no menu **Jobs & Pipelines**.
2. Adicione as tarefas `to_Bronze`, `to_Silver` e `to_Gold`.
3. Configure as dependências na ordem Bronze → Silver → Gold.
4. Selecione o compute para as tarefas.
5. Configure o agendamento ou utilize **Run now** para uma execução manual.
6. Confirme que as três tarefas terminam com o status **Succeeded**.

## Boas práticas aplicadas

- Uso de funções nativas do PySpark para manter o processamento distribuído.
- Operações idempotentes com escrita em modo `overwrite` e atualização de schema quando necessário.
- Tratamento defensivo de nulos e valores inválidos.
- Widgets para parametrização de caminhos e datas.
- Validações intermediárias nas camadas Bronze, Silver e Gold.
- Separação entre ingestão, transformação e consumo analítico.
- Orquestração declarada no `job.yaml`.

## Estrutura do repositório

```text
.
├── configuracao/
│   └── databricks/
│       └── job.yaml
├── evidencias/
│   └── workflow/
│       ├── successful-run.png
│       └── workflow-tasks.png
├── notebooks/
│   ├── 01_landing_to_bronze.ipynb
│   ├── 02_bronze_to_silver.ipynb
│   └── 03_silver_to_gold.ipynb
└── README.md
```

## Tecnologias

| Tecnologia | Utilização |
|---|---|
| Databricks | Execução dos notebooks e orquestração do pipeline |
| Apache Spark / PySpark | Processamento distribuído e transformações |
| Spark SQL | Consultas e criação das tabelas analíticas |
| Delta Lake | Persistência das tabelas no Lakehouse |
| API PTAX / Banco Central | Consulta da cotação histórica do dólar |
| Python | Lógica de ingestão, transformação e validação |
