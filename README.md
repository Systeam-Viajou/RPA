# RPA with PostgreSQL Automation

Este repositório contém uma aplicação Python destinada à automação RPA (Robotic Process Automation) para sincronizar dados entre dois bancos de dados PostgreSQL. Utiliza psycopg2 para conectar e manipular os dados.

## Estrutura do Projeto

- `rpa.py`: O script principal que realiza a sincronização de dados entre os bancos de dados.
- `requirements.txt`: Lista todas as dependências necessárias para rodar o projeto, incluindo `psycopg2` e `python-dotenv`.
- `dotenv_sample.env`: Um exemplo de arquivo .env para configuração das variáveis de ambiente necessárias.
- `.github/workflows/`: Contém workflows de GitHub Actions para automação de testes e deploy.
- `pull_request_template.md`: Template para criação de pull requests.

## Funcionalidade

  1. **Conexão com Bancos de Dados**: Estabelece conexões com dois bancos de dados PostgreSQL e MongoDB.
  2. **Sincronização de Dados**: Verifica diferenças de dados entre os bancos e sincroniza conforme necessário.
  3. **Logging**: Registra operações de inserção e atualização em logs para rastreabilidade.

## Configuração Inicial

1. Clone o repositório:
git clone https://github.com/Systeam-Viajou/RPA-PostgreSQL.git

2. Crie um arquivo `.env` com base no `dotenv_sample.env`.

3. Instale as dependências:
pip install -r requirements.txt

4. Execute a aplicação:
python rpa.py

#### Desenvolvido com ❤ e carinho pela equipe de análise de dados *Viajou*:

- [Gabriel Costa](https://github.com/gbrlscosta)
- [Gabrieli Oliveira](https://github.com/gabrieliolveira)
