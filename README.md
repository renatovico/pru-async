# 🐦 Pru-ASYNC

```
 _______  _______
(  ____ )(  ____ )|\     /|
| (    )|| (    )|| )   ( |
| (____)|| (____)|| |   | |
|  _____)|     __)| |   | |
| (      | (\ (   | |   | |
| )      | ) \ \__| (___) |
|/       |/   \__/(_______)/ASYNC
```

Mais uma submissão para a [Rinha de Backend 2025](https://github.com/zanfranceschi/rinha-de-backend-2025), desta vez em Ruby.

Fork do PRU DO https://github.com/leandronsp/pru do LeandroSP

Resolvi testar as gem async para ver como performaria sem sidekiq

## Requisitos

- Docker
- Make
- Força de vontade

## Setup

```bash
make api.setup
```

## Comandos úteis

### Gerenciamento dos Processadores de Pagamento

```bash
# Iniciar processadores de pagamento
make processors.up

# Testar endpoints dos processadores
make processors.test

# Limpar dados dos processadores
make processors.purge

# Parar processadores
make processors.down
```

### Desenvolvimento da API

```bash
# Iniciar ambiente de desenvolvimento
make start.dev

# Testar endpoints da API via NGINX (localhost:9999)
make api.test.payments
make api.test.summary
make api.test.purge

# Ver logs
make compose.logs

# Parar todos os serviços
make compose.down
```

### Teste de Performance

```bash
# Executar teste k6 (Rinha de Backend)
make rinha
```



----
Arte ASCII feita com [ASCII art generator](http://www.network-science.de/ascii/)
