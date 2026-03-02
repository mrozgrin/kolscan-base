# KOLScan Base API

Backend Node.js para rastreamento de KOLs (Key Opinion Leaders) na blockchain Base, inspirado no `kolscan.io`.

Este projeto fornece uma API RESTful para analisar a atividade de trading na blockchain Base, identificar traders influentes (KOLs) e gerar um leaderboard de performance. A análise é baseada puramente em dados on-chain, sem a necessidade de verificação de redes sociais.

## Funcionalidades

- **Indexação de Blocos:** Sincroniza e processa transações da blockchain Base em tempo real.
- **Análise de Swaps:** Decodifica eventos de swap de DEXes populares na Base (Uniswap, Aerodrome, etc.).
- **Cálculo de PnL:** Estima o lucro e prejuízo (PnL) de cada trade em USD.
- **Métricas de KOLs:** Calcula métricas de performance como taxa de vitória, lucro total e volume negociado.
- **Leaderboard:** Gera leaderboards diários, semanais e mensais dos traders mais lucrativos.
- **API RESTful:** Expõe todos os dados através de uma API completa com cache e rate limiting.

## Stack Tecnológica

- **Backend:** Node.js, Express, TypeScript
- **Banco de Dados:** PostgreSQL
- **Blockchain:** Ethers.js para interação com RPC da Base
- **Filas e Cache:** Redis (via Bull e IORedis)
- **Logging:** Winston

## Pré-requisitos

- Node.js >= 18.0.0
- PostgreSQL >= 13
- Redis >= 6
- Um endpoint RPC para a blockchain Base (ex: Alchemy, QuickNode)

## Instalação

1.  **Clone o repositório:**

    ```bash
    git clone https://github.com/mrozgrin/kolscan-base.git
    cd kolscan-base
    ```

2.  **Instale as dependências:**

    ```bash
    npm install
    ```

3.  **Configure as variáveis de ambiente:**

    Crie um arquivo `.env` na raiz do projeto, baseado no `.env.example`:

    ```bash
    cp .env.example .env
    ```

    Edite o arquivo `.env` com as suas configurações (banco de dados, RPC, etc.).

    ```dotenv
    # Server
    PORT=3000

    # Database (PostgreSQL)
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=kolscan_base
    DB_USER=postgres
    DB_PASSWORD=your_password

    # Redis
    REDIS_HOST=localhost
    REDIS_PORT=6379

    # Base Blockchain RPC URL
    BASE_RPC_URL=https://your-base-rpc-url.com
    ```

## Uso

1.  **Compile o código TypeScript:**

    ```bash
    npm run build
    ```

2.  **Execute as migrations do banco de dados:**

    Este comando criará todas as tabelas e índices necessários.

    ```bash
    npm run db:migrate
    ```

3.  **Inicie o servidor:**

    ```bash
    npm start
    ```

    O servidor da API e o indexador da blockchain serão iniciados.

### Scripts Disponíveis

-   `npm run dev`: Inicia o servidor em modo de desenvolvimento com `nodemon`.
-   `npm run dev:api-only`: Inicia apenas a API, sem o indexador.
-   `npm run build`: Compila o projeto TypeScript para JavaScript.
-   `npm run typecheck`: Verifica os tipos do projeto sem compilar.

## Estrutura do Projeto

```
src/
├── api/                # Lógica da API Express (controllers, routes, middleware)
├── config/             # Configurações da aplicação
├── database/           # Conexão com DB e migrations
├── indexer/            # Lógica de indexação da blockchain
├── jobs/               # Jobs em background (atualização de métricas)
├── services/           # Lógica de negócio (cálculo de métricas, preços)
├── types/              # Definições de tipos TypeScript
├── utils/              # Funções utilitárias (logger, helpers)
└── index.ts            # Ponto de entrada da aplicação
```

## Endpoints da API

-   `GET /api/health`: Status da API e do banco de dados.
-   `GET /api/leaderboard`: Leaderboard de KOLs.
    -   Query params: `period` (daily, weekly, monthly), `limit`, `page`.
-   `GET /api/kol/:address`: Detalhes de uma carteira específica.
-   `GET /api/kol/:address/swaps`: Histórico de swaps de uma carteira.
-   `GET /api/stats`: Estatísticas gerais da plataforma.
-   `GET /api/search?q=<query>`: Busca por endereço ou label.
-   `GET /api/tokens`: Lista os tokens mais negociados.
-   `GET /api/indexer/status`: Status do processo de indexação.

## Licença

Este projeto está licenciado sob a [Licença MIT](LICENSE).
