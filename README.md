# app_snapshot

Este repositorio funciona como camada publica de distribuicao de snapshots JSON utilizados pelo aplicativo Quero Investir.

## Regra de ownership

`data/fiis_fundamentals.json` e produzido exclusivamente por `Thiago-Alvees/projeto_app_quero_investir`.

Esse arquivo nao deve ser gerado, corrigido ou editado manualmente neste repositorio. A responsabilidade deste repositorio e distribuir artefatos aprovados pelo produtor canonico.

## Pipeline

O fluxo de fundamentos de FIIs segue esta cadeia:

```text
CVM
-> Thiago-Alvees/projeto_app_quero_investir
-> coleta
-> normalizacao
-> validacao fail-closed
-> Thiago-Alvees/app_snapshot
-> aplicativo
```

## Fail-closed

O produtor canonico gera um candidato e valida o resultado antes da publicacao. Se a validacao falhar, o snapshot publico anterior permanece intacto e nenhum candidato invalido deve ser publicado aqui.

## Arquivos em `data/`

- `data/fiis_fundamentals.json`: snapshot publico de fundamentos de FIIs produzido e validado pelo repositorio canonico.
- `data/fiis_snapshot.json`: snapshot de precos de FIIs.
- `data/market_snapshot.json`: snapshot de precos de ativos do mercado.
- `data/market_fundamentals.json`: snapshot de fundamentos de outros ativos do mercado.
- `data/events_feed.json`: snapshot publico de eventos usados pelo aplicativo.
- `data/latest/`: artefatos mais recentes publicados para consumo do aplicativo, incluindo manifesto, benchmarks e snapshot de mercado.

## Manutencao

Alteracoes de coleta, parsing, mapeamento, normalizacao ou Data Quality devem acontecer em `Thiago-Alvees/projeto_app_quero_investir`, nao neste repositorio.

Nao adicione neste repositorio coletores, validadores ou workflows alternativos para gerar `data/fiis_fundamentals.json`.
