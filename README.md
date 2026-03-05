# KOLSCAN Base

Backend Node.js para rastreamento de KOLs (Key Opinion Leaders) na blockchain Base. O sistema monitora carteiras em tempo real, detecta padrões de trading e calcula um **Follow Score** que indica o quão recomendável é seguir cada trader.

---

## Arquitetura

O projeto é composto por **dois programas independentes** que compartilham o mesmo banco de dados MySQL. Essa separação permite controlar o consumo de recursos de cada processo de forma independente e facilita a manutenção da lógica de análise sem precisar reiniciar o indexador.

```
Blockchain Base (RPC)
        │
        ▼
┌─────────────────────┐        ┌──────────────────────────┐
│  KOLSCAN INDEXER    │        │   KOLSCAN ANALYZER       │
│  (indexer-main.ts)  │        │   (analyzer-main.ts)     │
│                     │        │                          │
│  • Indexa blocos    │        │  • Calcula Follow Score  │
│  • Extrai swaps     │        │  • Atualiza kol_metrics  │
│  • Calcula PnL      │        │  • Detecta flags         │
│  • Grava no MySQL   │        │  • Job incremental (1h)  │
│  • Serve API REST   │        │  • Jobs diários (UTC)    │
│  • Preços (2 min)   │        │                          │
└────────┬────────────┘        └──────────┬───────────────┘
         │                                │
         └──────────┬─────────────────────┘
                    │
              ┌─────▼──────┐
              │   MySQL    │
              │ kolscan_db │
              └────────────┘
```

### Programa 1 — Indexer

Responsável por toda a comunicação com a blockchain. Conecta ao RPC da rede Base, processa blocos em tempo real, extrai eventos de swap das DEXs suportadas (Uniswap V2/V3, Aerodrome), calcula o PnL por posição (modelo compra→venda com custo médio ponderado) e grava os dados brutos no MySQL. Também serve a API REST e mantém os preços dos tokens atualizados.

### Programa 2 — Analyzer

Responsável por toda a lógica de análise e pontuação. Lê os dados brutos gravados pelo Indexer e calcula o Follow Score com seus quatro componentes (Followability, Consistência, PnL e Win Rate). Roda em jobs agendados para não competir com o Indexer por recursos do banco. Não requer conexão com a blockchain.

---

## Pré-requisitos

| Serviço | Versão mínima | Observação |
|---|---|---|
| **Node.js** | 18.0.0 | Recomendado: 20 LTS |
| **MySQL** | 8.0 | Banco de dados principal |
| **Redis** | 6.0 | Cache e filas |
| **RPC Base** | — | Endpoint de acesso à blockchain Base |

Para o RPC, recomenda-se o [Alchemy](https://alchemy.com) (plano gratuito disponível) ou [QuickNode](https://quicknode.com). O endpoint público `https://mainnet.base.org` pode ser usado para testes, mas é mais lento e sujeito a rate limiting.

---

## Instalação

### 1. Clonar e instalar dependências

```bash
git clone https://github.com/mrozgrin/kolscan-base.git
cd kolscan-base
npm install
```

### 2. Criar o banco de dados MySQL

```sql
CREATE DATABASE kolscan_base CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 3. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Edite o arquivo `.env` com seus valores. Os campos essenciais são:

```dotenv
# Banco de dados
DB_HOST=localhost
DB_PORT=3306
DB_NAME=kolscan_base
DB_USER=root
DB_PASSWORD=sua_senha

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# RPC da blockchain Base
BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/SUA_CHAVE

# Bloco inicial — use um bloco recente para não indexar desde o início
# Consulte o bloco atual em: https://basescan.org
START_BLOCK=30000000
```

> **Sobre `START_BLOCK`:** a blockchain Base possui dezenas de milhões de blocos. Definir um bloco recente (por exemplo, dos últimos 7 dias) evita que o indexador tente processar anos de histórico, o que levaria dias ou semanas.

### 4. Rodar as migrations

```bash
npm run db:migrate
```

Cria todas as tabelas e índices necessários. Pode ser executado com segurança múltiplas vezes — as migrations são idempotentes.

---

## Uso

### Iniciar o Indexer (Terminal 1)

```bash
# Desenvolvimento (com hot-reload)
npm run indexer:dev

# Apenas API, sem indexar blocos (útil para consultas)
npm run indexer:api-only

# Produção (requer npm run build antes)
npm run build && npm run indexer
```

O Indexer sobe a API REST em `http://localhost:3000` e começa a processar blocos da blockchain. O terminal exibe o progresso em tempo real:

```
[SYNC] 42.3% | bloco 28450123 | 12.5 blocos/s | ETA: 8m32s
[BLOCK 28460001] 2026-03-05 14:22:11 | 3 tx(s) → MySQL ✓
  [BUY ] 0x163d54... 5.7034 WETH → 11771.5597 CHARLES → MySQL ✓
  [SELL] 0xb6f182... 37752.4547 CHARLES → 20.9711 WETH | PnL: +3.098884 WETH (✓) → MySQL ✓
```

### Iniciar o Analyzer (Terminal 2)

```bash
# Desenvolvimento
npm run analyzer:dev

# Produção
npm run build && npm run analyzer
```

O Analyzer não precisa de RPC nem de internet — apenas acesso ao MySQL. Pode ser iniciado antes ou depois do Indexer, em qualquer ordem.

```
[analyzer-jobs] Incremental metrics update → every 1 hour
[analyzer-jobs] Flags detection → próxima execução: 02:00 UTC (em 6h 12min)
[analyzer-jobs] Partial score recalculation → próxima execução: 03:00 UTC (em 7h 12min)
```

---

## Jobs agendados

### Indexer

| Job | Frequência | Descrição |
|---|---|---|
| Atualização de preços | A cada 2 min | Busca preço atual de todos os tokens na tabela `tokens` |
| Market data | A cada 5 min | Atualiza volume, liquidez e market cap dos tokens ativos |

### Analyzer

| Job | Frequência | Descrição |
|---|---|---|
| Incremental | A cada 1 hora | Recalcula métricas de wallets com swaps nas últimas 2h |
| Flags | 02:00 UTC diário | Detecta Scalper, Bundler, Creator-Funded e Sybil |
| Scores parcial | 03:00 UTC diário | Recalcula follow score de wallets ativas nas últimas 48h |

---

## Ferramentas de linha de comando

### Ver top traders no terminal

```bash
npm run top-traders                        # top 100, últimos 30 dias (padrão)
npm run top-traders -- --limit 50          # top 50
npm run top-traders -- --period 7          # últimos 7 dias
npm run top-traders -- --limit 10 --period 90
```

Exibe follow score, PnL percentual diário, win rate, holding médio e todos os componentes do score para cada trader.

### Recalcular PnL de todas as wallets

Necessário após a primeira instalação ou quando a lógica de cálculo de PnL for alterada. **Pode levar horas** dependendo do volume de dados.

```bash
npm run recalculate-pnl
```

### Testar recálculo com 3 wallets (amostra)

Antes de rodar o recálculo completo, valide a lógica com uma amostra editando as wallets no arquivo `src/scripts/recalculate-pnl-amostra.ts`:

```bash
npm run recalculate-pnl-amostra
```

O script exibe o fluxo detalhado de cada trade (BUY/SELL/SWAP), o custo médio acumulado e o PnL calculado, permitindo verificar se os valores estão corretos antes de processar toda a base.

---

## API REST

A API é servida pelo Indexer na porta configurada em `PORT` (padrão: `3000`).

| Endpoint | Descrição |
|---|---|
| `GET /api/health` | Status do sistema |
| `GET /api/leaderboard` | Top traders por follow score |
| `GET /api/kol/:address` | Detalhes completos de uma wallet |
| `GET /api/kol/:address/swaps` | Histórico de swaps |
| `GET /api/kol/:address/transactions` | Histórico de transações |
| `GET /api/tokens` | Tokens indexados |
| `GET /api/stats` | Estatísticas gerais |
| `GET /api/indexer/status` | Status do indexador |

---

## Estrutura do banco de dados

| Tabela | Descrição |
|---|---|
| `wallets` | Carteiras detectadas, com flags de comportamento |
| `swap_events` | Todos os swaps indexados, com `pnl_base`, `pnl_pct` e `swap_type` |
| `positions` | Posições abertas por wallet/token (custo médio ponderado) |
| `transactions` | Transações brutas da blockchain |
| `tokens` | Cache de metadados e preços dos tokens |
| `kol_metrics` | Métricas agregadas por wallet e período (`daily`, `weekly`, `monthly`, `all_time`) |
| `kol_score_history` | Histórico mensal de follow scores |
| `sybil_clusters` | Grupos de wallets com comportamento coordenado |
| `sybil_cluster_members` | Membros de cada cluster Sybil |
| `indexer_state` | Estado do indexador (último bloco processado) |
| `schema_migrations` | Controle de versão do banco |

---

## Cálculo de PnL

O PnL é calculado **na moeda base da posição** — o token que foi usado para comprar — e não em USD. O modelo usa custo médio ponderado (VWAP) para acumular múltiplas compras antes de calcular o lucro na venda. Qualquer par de tokens é suportado: WETH→CHARLES→WETH, VIRTUAL→PEPE→VIRTUAL, PEPE→CHARLES→PEPE, etc.

**Exemplo:**

```
Compra 1: 5.70 VIRTUAL → 11.771 CHARLES  → custo médio: 0.000484 VIRTUAL/CHARLES
Compra 2: 2.07 VIRTUAL →  4.283 CHARLES  → custo médio: 0.000484 VIRTUAL/CHARLES (VWAP)
Venda:   37.752 CHARLES → 20.97 VIRTUAL
         custo proporcional = 0.000473 × 37.752 = 17.87 VIRTUAL
         PnL = 20.97 − 17.87 = +3.09 VIRTUAL (+17.3%)
```

O campo `pnl_base` em `swap_events` armazena o PnL na moeda base. O campo `pnl_pct` armazena o percentual de lucro. Compras e swaps meme→meme têm esses campos como `NULL`.

---

## Follow Score

O Follow Score (0–100) indica o quão recomendável é seguir uma carteira. É composto por quatro componentes com pesos distintos:

| Componente | Peso | O que mede |
|---|---|---|
| **Followability** | 40% | Copiabilidade: hold time adequado, volume razoável, liquidez |
| **Consistência** | 25% | Estabilidade do win rate ao longo do tempo, diversificação |
| **PnL** | 20% | Lucro real comparado ao P90 de todas as carteiras |
| **Win Rate** | 15% | Percentual de trades lucrativos |

| Score | Classificação |
|---|---|
| ≥ 80 | Excelente — altamente recomendado seguir |
| 70–79 | Muito Bom |
| 60–69 | Bom — vale a pena acompanhar |
| 50–59 | Aceitável — siga com atenção |
| 40–49 | Fraco — scalper ou baixo win rate |
| < 40 | Não recomendado |

---

## Fluxo de operação recomendado

### Primeira instalação

```bash
# 1. Configurar banco e migrations
npm run db:migrate

# 2. Iniciar o Indexer (Terminal 1)
npm run indexer:dev

# 3. Aguardar o catch-up histórico (acompanhe o progresso no terminal)
#    O tempo depende do START_BLOCK configurado.

# 4. Iniciar o Analyzer (Terminal 2)
npm run analyzer:dev

# 5. Após o catch-up, recalcular PnL de toda a base
npm run recalculate-pnl

# 6. Verificar os top traders
npm run top-traders
```

### Operação contínua

Após a primeira instalação, basta manter os dois processos rodando. O Indexer processa novos blocos automaticamente e o Analyzer atualiza as métricas a cada hora.

---

## Reiniciar o banco do zero

Se precisar começar do zero (apagar todos os dados e reindexar):

```bash
# 1. Parar os dois processos (Ctrl+C em cada terminal)

# 2. No MySQL, recriar o banco
mysql -u root -p -e "DROP DATABASE kolscan_base; CREATE DATABASE kolscan_base CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# 3. Rodar as migrations novamente
npm run db:migrate

# 4. Ajustar START_BLOCK no .env para o bloco desejado

# 5. Reiniciar o Indexer e o Analyzer
npm run indexer:dev   # Terminal 1
npm run analyzer:dev  # Terminal 2
```

---

## Stack tecnológica

| Camada | Tecnologia |
|---|---|
| Runtime | Node.js 18+ / TypeScript |
| Framework HTTP | Express 5 |
| Banco de dados | MySQL 8 (via mysql2) |
| Cache e filas | Redis (via IORedis e Bull) |
| Blockchain | Ethers.js 6 |
| Precisão numérica | Decimal.js 10 |
| Logging | Winston |

---

## Licença

Este projeto está licenciado sob a [Licença MIT](LICENSE).
