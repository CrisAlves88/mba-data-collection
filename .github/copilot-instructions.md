# Copilot Instructions for AI Coding Agents

## Visão Geral do Projeto
Este repositório é um template para ambientes de dados usando Python, PySpark, Pandas e PostgreSQL, com Dev Containers e GitHub Codespaces. O objetivo é garantir ambiente reprodutível e pronto para uso, facilitando o desenvolvimento colaborativo e a integração de dados.

## Estrutura Principal
- **src/**: Scripts de exemplo para testes de integração com Pandas, Spark e PostgreSQL.
- **requirements.txt**: Dependências Python do projeto.
- **.devcontainer/**: (não listado, mas referenciado) Configuração do ambiente Docker/VS Code.

## Fluxos de Trabalho Essenciais
- **Testes de ambiente**: Execute os scripts em `src/` para validar integração:
  - `python src/teste_pandas.py` — Testa manipulação de DataFrames.
  - `python src/teste_spark.py` — Testa processamento Spark.
  - `python src/teste_postgres.py` — Testa conexão com PostgreSQL.
- **Git workflow**:
  - `git status` → `git add .` → `git commit -m "mensagem"` → `git push`
- **Conexão com PostgreSQL**:
  - Use o host `db`, usuário `myuser`, senha `mypassword`, porta `5432`, banco `mydb` (veja `.devcontainer/docker-compose.yml`).
  - Conecte via extensão de banco de dados do VS Code (ícone de cilindro).

## Convenções e Padrões
- Scripts de teste e integração ficam em `src/`.
- Mensagens de commit devem ser claras e descritivas.
- O ambiente é isolado via Docker, não dependa de configurações locais.
- Para consultas SQL, sempre use o host `db` (não `localhost`).

## Integrações e Dependências
- **Python 3.11**
- **PySpark**
- **Pandas**
- **PostgreSQL** (acessível via Docker Compose)
- **Docker** (gerencia containers)
- **VS Code Extensions** (pré-instaladas para banco de dados)

## Exemplos de Uso
- Para validar ambiente: `python src/teste_pandas.py`, `python src/teste_spark.py`, `python src/teste_postgres.py`
- Para adicionar novo script: crie em `src/`, adicione ao `requirements.txt` se necessário, e siga o fluxo de commit/push.

## Recomendações para Agentes
- Priorize automação e reprodutibilidade.
- Sempre valide integração entre componentes após alterações.
- Documente padrões específicos deste projeto ao sugerir mudanças.
- Consulte o `README.md` para detalhes de configuração e onboarding.

---

Seções incompletas ou dúvidas? Solicite feedback para aprimorar as instruções.
