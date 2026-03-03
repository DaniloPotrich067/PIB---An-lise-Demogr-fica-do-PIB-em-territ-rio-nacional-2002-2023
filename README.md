# Análise Demográfica - Dashboard de PIB Municipal

Este repositório contém um pipeline de ETL e um dashboard em Streamlit para análise do Produto Interno Bruto municipal do Brasil.

## Estrutura do projeto

- **`ETL/`**: módulos Python responsáveis por baixar, transformar e carregar dados do IBGE/SIDRA.
  - `extract.py`: consultas HTTP robustas às APIs do IBGE e SIDRA, com retentativas e organização de arquivos JSON em `DATA/`.
  - `transform.py`: limpeza e normalização das tabelas de localidades (regiões, UFs, municípios) e das séries de PIB/VAB do SIDRA.
  - `load.py`: cria engine SQLAlchemy e realiza upserts no PostgreSQL; inclui rotina de sanidade.

- **`SQL/`**: scripts de definição do esquema e materialização de visões analíticas.
  - `00_Reset_Schema.sql`: limpa o banco.
  - `01_query.sql`: cria dimensões, fatos, índices e views materializadas usadas pelo dashboard.
  - `02_tests.sql`: consultas de validação (qualidade, cobertura, etc.).

- **`UI/`**: aplicação Streamlit para visualização interativa dos dados.
  - `app.py`: launcher principal com navegação entre páginas.
  - `COMPONENTES/`: funções reutilizáveis para filtros, gráficos, layouts e mapas.
  - `PÁGINAS/`: seis pages que respondem a perguntas específicas:
    - **00_Extrair** - painel de controle da ETL (extrair, transformar, carregar).
    - **01_Dashboard** - visão executiva com indicadores, mapa e top municípios/UFs.
    - **02_Temporal** - evolução do PIB e VAB ao longo dos anos.
    - **03_Composicao** - composição setorial do VAB por estado/ano.
    - **04_Distribuicao** - distribuição de PIB por faixa e comparação entre UF/média.
    - **05_Concentracao** - participação percentual de municípios no PIB estadual.
    - **06_Dados** - exportação, qualidade e cobertura dos dados.

## Funcionalidades principais

1. **ETL completo idempotente**: 
   - Dados de localidades (regiões, UFs, municípios) e PIB/VAB obtidos via APIs públicas.
   - Arquivos JSON são validados (`_is_valid_json`) para evitar re-downloads.
   - Transformações vetorizadas em pandas e padronização de tipos.
   - Carga incremental ou reset com upserts eficientes em PostgreSQL.
   - Painel de controle com progresso e opções de período/variáveis.

2. **Banco de dados**:
   - Esquema dimensional star com `dim_regiao`, `dim_uf`, `dim_municipio`, `dim_variavel` e `fato_indicador_municipio`.
   - Índices otimizados para consultas de dashboard.
   - Views materializadas (`mart.*`) para cálculos agregados e análises prontas (PIB por UF/ano, concentração, composição etc.).
   - Arquivo `pib_backup.dump` disponibiliza um snapshot exportado para restauração na nuvem.

3. **Validações**:
   - Scripts de teste (`SQL/02_tests.sql`) com contagens e checagens de integridade.
   - Função `sanity` em `load.py` retorna métricas (número de municípios, variáveis e registros).
   - ETL evita arquivos vazios/corrompidos e oferece mensagens de erro detalhadas.

4. **Conexão com banco em nuvem**:
   - A aplicação Streamlit usa `st.secrets["connections"]["pib"]` para obter credenciais.
   - O pacote `load.make_engine_from_secrets` facilita a criação de `sqlalchemy.Engine` usando as mesmas configurações.
   - Basta configurar as variáveis `host`, `port`, `username`, `password` e `database` no `secrets.toml` ou no ambiente do Streamlit Cloud.

## Descrição das páginas e dados apresentados

| Página | Métricas / gráficos | O que responde |
|--------|---------------------|----------------|
| **Dashboard** | KPIs (PIB total, cobertura, municípios, UFs, top1/top5 shares); mapa de UFs colorido por PIB; gráficos de top municípios e top UFs | Qual é o tamanho e a cobertura dos dados? Quais são os maiores municípios/UFs no recorte atual? |
| **Análise Temporal** | Série histórica de PIB/variáveis; comparação ano a ano e contra a média nacional | Como o PIB/VAB evolui ao longo do tempo? Há tendências ou choques? |
| **Composição** | Barras empilhadas por setor (agrícola, indústria, serviços, adm. pública, impostos) por UF/ano | Qual a participação setorial do VAB em cada estado? Como varia no tempo? |
| **Distribuição** | Histograma de municípios por faixas de PIB; boxplot comparando UFs com médias | Como o PIB municipal se distribui? Quais estados têm maior dispersão? |
| **Concentração** | Pareto/top municípios; percentual de participação dentro da UF; mapa | Quanto os PIBs estão concentrados? Quem são os municípios líderes? |
| **Dados & Qualidade** | Tabela exportável; métricas de cobertura e ausência; evolução das linhas extraídas | Qual a qualidade dos dados? Quais anos/variáveis faltam? Há anomalias? |

> 📝 A página **00_Extrair** serve apenas durante a fase de ingestão; não é parte do dashboard final.

## Como rodar

1. **Configuração de ambiente**
   ```bash
   python -m venv venv
   venv\Scripts\activate          # Windows
   pip install -r requeriments.txt  # ou criar um requirements.txt com as dependências
   ```
2. **Conectar ao banco**
   - Se tiver `pib_backup.dump` use `pg_restore` para restaurar o banco no servidor desejado.
   - Alternativamente, execute o painel `00_Extrair.py` em ambiente local para fazer ETL direto na nuvem.
   - Configure `UI/.streamlit/secrets.toml` (ou variável de ambiente) com:
     ```toml
     [connections.pib]
     host = "seu-host"
     port = "5432"
     database = "pib"
     username = "usuario"
     password = "senha"
     ```
3. **Executar o dashboard**
   ```bash
   streamlit run UI/app.py
   ```

## Observações

- **Limitações de dados**: a tabela SIDRA 5938 oferece VAB setorial apenas até 2021; 2022–2023 contêm apenas PIB total. O ETL filtra automaticamente combinações inválidas e as páginas de Composição/Temporal tratam isso.
- **Resiliência**: o ETL é idempotente; arquivos já válidos não são baixados novamente. Erros de rede são tratados com re‑tentativas exponenciais.
- **Implantação**: pode ser hospedado em plataformas como Streamlit Cloud, Heroku ou qualquer servidor que execute Python e PostgreSQL. Ajuste as configurações de SSL no `make_engine` conforme necessário.

---

Esse README deve servir como documentação inicial para qualquer pessoa que queira entender ou implantar o projeto. инсан