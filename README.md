# Pipeline de Dados para Coleta e Armazenamento de Dados de Ações

Este projeto implementa um pipeline de dados para coletar e armazenar informações sobre ações da NASDAQ. O fluxo de trabalho é dividido em três etapas principais:

## Situação problema
A InvestAnalytica, uma renomada firma de consultoria de investimentos, enfrenta um desafio significativo no cenário volátil do mercado de ações. A empresa precisa de uma aneira mais eficiente e precisa de analisar o vasto volume de dados do mercado de ações para fornecer recomendações atualizadas e confiáveis aos seus clientes. Atualmente, a análise de dados é feita manualmente por uma equipe de analistas, o que leva a atrasos e
potenciais erros humanos. 

**Solução Proposta**: *Plataforma de Análise do Mercado de Ações*

A solução proposta é uma plataforma de análise do mercado de ações que colete,
armazene e processe dados de ações em tempo real, fornecendo insights precisos
e atualizados para os clientes da InvestAnalytica. A plataforma será composta por três componentes principais:

1. **Coleta de Dados**: um pipeline de dados que colete informações sobre ações
da NASDAQ em tempo real.
2. **Armazenamento de Dados**: um banco de dados que armazene os dados
coletados em uma estrutura organizada e escalável.
3. **Análise de Dados**: um sistema de análise que processe os dados
armazenados e forneça insights precisos e atualizados para os clientes.
A plataforma será desenvolvida utilizando tecnologias de ponta, como Apache
Airflow e POSTGRES, garantindo escalabilidade, performance e segurança. 

A solução proposta resolverá o problema da InvestAnalytica, fornecendo uma
análise mais eficiente e precisa do mercado de ações, permitindo que a empresa fornça recomendações atualizadas e confiáveis aos seus clientes.
## Pipeline de Dados
O pipeline de dados é responsável por coletar informações sobre ações da NASDAQ em tempo real. A arquitetura do pipeline é composta por três etapas principais:



## 1. Coleta e Armazenamento (dag_consulta_api)

- **Descrição:**
  - Essa etapa é acionada uma vez ao dia, após o fechamento da bolsa.
  - Acessa arquivos JSON com dados de ações da NASDAQ por meio da API do Polygon.io.
  - Verifica se esses dados já estão no banco de dados PostgreSQL (usando uma consulta SQL com base na data) para evitar duplicação.
  - Armazena os dados em uma camada "raw" (ou "bruta"), que é um blob no Azure (um container de armazenamento de objetos).

## 2. Sensor (dag_sensor)

- **Descrição:**
  - O sensor é responsável por monitorar o container de armazenamento.
  - A cada 30 minutos, ele verifica se há novos arquivos no container.
  - Sempre que um novo arquivo é detectado, inicia a próxima DAG (DAG de transformação e carregamento).

## 3. Transformação e Carregamento (dag_transf_carregar)

- **Descrição:**
  - Essa etapa é acionada pela DAG do sensor.
  - Acessa o bucket via API.
  - Recebe a lista de arquivos e quebra as strings dos nomes (que seguem o padrão 'preco_acoes-YYYY-MM-DD.json').
  - Gera uma lista com as datas.
  - Conferência adicional com uma consulta (distinct dates).
  - Carrega os arquivos em um DataFrame.
  - Realiza as transformações necessárias usando o Pandas.
  - Carrega os DataFrames no banco de dados PostgreSQL.

## Configuração

- Certifique-se de configurar suas credenciais para a API do Polygon.io e para o banco de dados PostgreSQL.
- Atualize os detalhes de conexão no código das DAGs, ou em suas variáveis de ambiente do Airflow.

## Considerações finais

Projeto em grupo: Claudiane Amaral, Elaine Priscila, Decio Morais, Henrique Teixeira e Leonildo Linck.

Este projeto foi desenvolvido com base em conceitos aprendidos em aula, visando simplicidade e eficiência. Para eventuais dúvidas ou melhorias, sinta-se à vontade para entrar em contato.

E-mail: leonildolinck@gmail.com Discord: leonildo
