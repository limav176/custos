# Projeto dominio Custos Cloud

Este projeto de DataLake, denominado "Custos Cloud", utiliza o SQL Athena para ETL e é construído com o `dbt-athena-community`. Abaixo estão as instruções detalhadas sobre a estrutura do projeto, configuração de arquivos essenciais, instalação do `dbt-athena-community` e execução dos principais comandos do dbt.

## Estrutura de Pastas do `dbt-athena-community`

A estrutura de pastas típica em um projeto `dbt-athena-community` é a seguinte:

- `models/`: Contém os modelos SQL do dbt. Cada arquivo SQL representa um modelo.
- `tests/`: Armazena os testes que podem ser executados nos modelos para garantir a integridade dos dados.
- `data/`: Usada para dados que podem ser carregados diretamente no Athena.
- `macros/`: Armazena macros que podem ser usadas para estender a funcionalidade do dbt.
- `analysis/`: Contém scripts SQL para análise ad-hoc que não são implantados no banco de dados.
- `snapshots/`: Utilizado para capturar a versão dos dados em um momento específico.

## Arquivos de Configuração

### `source.yaml`

Localizado no diretório `models/`, define as fontes de dados (tabelas, vistas, etc.) para o dbt. Permite especificar metadados como descrição, colunas, testes e mais.

### `project.yaml`

O coração do projeto dbt, localizado na raiz do projeto. Define a configuração geral do projeto, incluindo nome, versão do dbt, configurações de modelo, e mais.

### `profiles.yaml`

Este arquivo, geralmente localizado no diretório `~/.dbt/`, contém as configurações de conexão com o banco de dados, como host, usuário, senha e outros parâmetros necessários para o dbt se conectar ao Athena.

## Instalação do `dbt-athena-community` versão 1.5.2

1. Certifique-se de que o Python está instalado em sua máquina.
2. Instale o `dbt-athena-community` usando pip:

   ```bash
   pip install dbt-athena-community==1.5.2
   ```

## Executando Comandos Principais do dbt

1. **dbt run**: Executa os modelos dbt e aplica as transformações no banco de dados.

   ```bash
   dbt run
   ```

2. **dbt compile**: Compila os scripts SQL dos modelos dbt sem executá-los, útil para verificar erros de sintaxe.

   ```bash
   dbt compile
   ```

3. **dbt debug**: Verifica a configuração do seu projeto e a conexão com o banco de dados, ajudando a identificar problemas de configuração.

   ```bash
   dbt debug
   ```

## Conclusão

Este README fornece uma visão geral do projeto "Custos Cloud" usando o `dbt-athena-community`. Para mais detalhes e personalização, consulte a documentação oficial do dbt e do Athena.
