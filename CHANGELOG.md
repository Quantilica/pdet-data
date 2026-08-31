# Changelog

## [0.5.2] - 2026-08-31
### Corrigido
- Quitação de dívida de lint (E501/docstrings longas) herdada dos sweeps de
  documentação de 2026-08-14; nenhum comportamento alterado.

## [0.5.1] - 2026-08-10
### Corrigido
- Atualizada dependência `quantilica-core` para `>=0.5.0` devido às novas assinaturas requeridas pelo CLI.
- Adicionada dependência implícita do `polars>=1.0.0` requerida pelo `reader.py`.

## [0.5.0] - 2026-08-10
### Alterado
- Migração completa da CLI para utilização da SDK unificada (`FetcherApp`).
- Remoção do gerenciamento manual de cliente FTP (`ftplib`) em favor do `FtpClient` provido pelo `quantilica-core`.

## [0.4.0] - 2026-08-07
### Alterado
- Refatoração arquitetural: Remoção de dependências (`quantilica-cli` e `quantilica-catalog`) e limpeza de imports. Os fetchers agora são pacotes de extração puros, dependendo estritamente do `quantilica-core`.

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.2.0] - 2026-05-19

Primeira entrada em formato Keep a Changelog; documenta o estado do pacote nesta
versão.

### Adicionado

- Coleta de microdados de RAIS e CAGED via FTP da PDET, com leitura inteligente
  (detecção de formato, codificação e estrutura) e correção de CSVs "ragged".
- Conversão de tipos (strings → números, booleanos e categóricas) e processamento
  com Polars.
- CLI standalone e plugin Typer para o `quantilica-cli` (`quantilica pdet`).
