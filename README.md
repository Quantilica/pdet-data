# pdet-fetcher: Microdados de emprego e trabalho da PDET

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.10+-blue.svg?style=flat-square)

> Ferramenta para coletar, processar e analisar microdados de emprego e trabalho do Brasil diretamente da **PDET** (Plataforma de Disseminação de Estatísticas do Trabalho).

**Para a documentação completa, consulte [https://docs.quantilica.com](https://docs.quantilica.com).**

Acesse dados do **RAIS** e **CAGED** de forma programática, com conversão automática de tipos, tratamento de anomalias e exportação para formatos modernos como Parquet.

---

## O que é a PDET?

A **PDET** é a plataforma oficial do Ministério do Trabalho e Previdência Social (MTPS) que disponibiliza os microdados de trabalho do Brasil. Ela mantém as bases:

- **RAIS** (Relação Anual de Informações Sociais): Dados de todos os vínculos de emprego formal registrados anualmente
- **CAGED** (Cadastro Geral de Empregados e Desempregados): Movimentações mensais de emprego (admissões, demissões, etc.)

Esses microdados são essenciais para análises de mercado de trabalho, pesquisa acadêmica e inteligência de negócios.

---

## Por que usar `pdet-fetcher`?

### Desafios com os dados brutos

Os microdados da PDET vêm em formatos legados (7z, ZIP) com características específicas:

- Colunas com tipos de dados mal definidos (números com espaços e pontos)
- Arquivos CSV "ragged" com número inconsistente de colunas
- Codificações de caracteres variáveis
- Nomes e estruturas de colunas diferentes por ano e dataset

### O que essa ferramenta oferece

**Acesso direto via FTP**: Conecta automaticamente ao servidor da PDET e baixa todos os arquivos  
**Leitura inteligente**: Detecta formato, codificação e estrutura automaticamente  
**Conversão de tipos**: Transforma strings em números, booleanos e categóricas  
**Correção de dados**: Fixa CSVs ragged, trata valores faltantes  
**Performance**: Usa [Polars](https://pola.rs/) para processamento rápido  
**Sem dependências pesadas**: Apenas `polars` e `tqdm`

---

## Instalação

```bash
pip install git+https://github.com/Quantilica/pdet-fetcher.git
```

**Requisitos**: Python 3.10+ e `7z` no PATH (usado por `convert_*` para extrair os arquivos baixados).

---

## CLI

O pacote instala o comando `pdet-fetcher` (também acessível via `python -m pdet_fetcher`) com cinco subcomandos:

```text
pdet-fetcher <subcommand> [args]

Subcommands:
  sync     [DATASETS...] [-o DIR]   Sincroniza RAIS e CAGED (dados + docs). Sem
                                    DATASETS, baixa rais, caged e caged-2020.
  list     [-o DIR]                 Lista no FTP o que ainda falta baixar
  convert  -i DATA_DIR [-o DIR]     Descompacta e converte CSVs em Parquet
  columns  DATASET -i DATA_DIR [-o OUT_DIR]
                                    Extrai os nomes de colunas de cada arquivo
  pipeline [DATASETS...] [-o DIR] [--parquet-dir DIR]
                                    Pipeline completo: sync -> convert
```

`DATASET` em `columns` aceita: `rais-vinculos`, `rais-estabelecimentos`, `caged`, `caged-ajustes`, `caged-2020`.

---

## Uso Rápido

### Baixar todos os microdados

```bash
pdet-fetcher sync -o ./dados
```

Equivalente em Python:

```python
from pathlib import Path
from pdet_fetcher import (
    connect,
    fetch_rais, fetch_rais_docs,
    fetch_caged, fetch_caged_docs,
    fetch_caged_2020, fetch_caged_2020_docs,
)

ftp = connect()
try:
    fetch_rais(ftp=ftp, dest_dir=Path("./dados"))
    fetch_rais_docs(ftp=ftp, dest_dir=Path("./dados"))
    fetch_caged(ftp=ftp, dest_dir=Path("./dados"))
    fetch_caged_docs(ftp=ftp, dest_dir=Path("./dados"))
    fetch_caged_2020(ftp=ftp, dest_dir=Path("./dados"))
    fetch_caged_2020_docs(ftp=ftp, dest_dir=Path("./dados"))
finally:
    ftp.close()
```

### Ler dados RAIS

```python
from pathlib import Path
import polars as pl
from pdet_fetcher.reader import read_rais

# Ler vínculos de emprego (ano 2023)
df = read_rais(
    filepath=Path("dados/rais_2023_vinculos.csv"),
    year=2023,
    dataset="vinculos"
)

print(df.schema)
print(df.head())

# Explorar: Qual é o setor com mais empregados?
top_setores = df.group_by("cnae_setor").agg(
    pl.col("id_vinculo").count().alias("n_vinculos")
).sort("n_vinculos", descending=True).head(10)

print(top_setores)
```

### Ler dados CAGED (clássico ou 2020+)

`read_caged` lê todas as variantes através do parâmetro `dataset`
(`caged`, `caged-ajustes`, `caged-2020-mov`, `caged-2020-for`, `caged-2020-exc`):

```python
from pathlib import Path
import polars as pl
from pdet_fetcher.reader import read_caged

# CAGED clássico (até 2019)
df_caged = read_caged(
    Path("dados/caged_201812.csv"),
    date=201812,
    dataset="caged",
)

# Novo CAGED — movimentações de janeiro/2024
df_mov = read_caged(
    Path("dados/cagedmov_202401.csv"),
    date=202401,
    dataset="caged-2020-mov",
)

# Analisar: saldo de emprego por UF
saldo = df_mov.with_columns(
    saldo = pl.col("admissoes") - pl.col("demissoes")
).group_by("uf").agg(
    pl.col("saldo").sum()
).sort("saldo", descending=True)

print(saldo)
```

### Processar em lote

```python
from pathlib import Path
from pdet_fetcher.reader import read_rais
import polars as pl

# Ler múltiplos anos de RAIS
anos = [2020, 2021, 2022, 2023]
dfs = []

for ano in anos:
    df = read_rais(
        filepath=Path(f"dados/rais_{ano}_vinculos.csv"),
        year=ano,
        dataset="vinculos"
    )
    dfs.append(df)

# Concatenar e analisar série temporal
df_completo = pl.concat(dfs)

evolucao = df_completo.group_by("ano").agg(
    total_vinculos=pl.col("id_vinculo").count()
).sort("ano")

print(evolucao)
```

---

## Dados Disponíveis

### RAIS (Relação Anual de Informações Sociais)

**O que é**: Base de referência com todos os vínculos de emprego formal do Brasil, declarados anualmente pelos empregadores.

**Frequência**: Anual (dezembro de cada ano)

**Datasets**:

- **Vínculos**: Informações sobre cada relação de emprego (salário, ocupação, setor, etc.)
- **Estabelecimentos**: Dados das empresas/órgãos (CNPJ, endereço, setor econômico)

**Períodos**: 1985 até o presente

**Casos de uso**:

- Análise de renda por região, setor e ocupação
- Estudos de empregabilidade
- Pesquisa sobre desigualdade de gênero/raça no trabalho formal
- Análise de dinâmica de empresas

---

### CAGED (Cadastro Geral de Empregados e Desempregados)

**O que é**: Registro de fluxo mensal de emprego, com informações sobre admissões e demissões declaradas pelos empregadores.

**Frequência**: Mensal

**Datasets** (versão clássica, até dezembro/2019):

- **Ajustes**: Correções de dados fora do prazo
- **Movimentações**: Admissões, demissões e outras movimentações

**Períodos**: 1985 até dezembro de 2019

**Caso de uso**:
- Indicadores conjunturais de emprego (saldo de vagas)
- Análise de flutuações cíclicas do mercado de trabalho
- Monitoramento em tempo real da economia

---

### CAGED 2020 (Novo CAGED)

**O que é**: Nova versão do CAGED com estrutura de dados modernizada, implementada a partir de janeiro/2020.

**Frequência**: Mensal

**Datasets**:

- **Movimentações**: Admissões, demissões, movimentações declaradas dentro do prazo
- **Fora do Prazo**: Movimentações declaradas depois do período de referência
- **Exclusões**: Cancelamento de movimentações (correções feitas retroativamente)

**Períodos**: Janeiro de 2020 até o presente

**Melhorias em relação ao CAGED clássico**:

- Estrutura de dados mais consistente
- Melhor tratamento de correções (exclusões separadas)
- Informações mais detalhadas sobre ocupações

**Caso de uso**:

- Mesmas análises conjunturais da versão anterior
- Análises mais granulares com a nova estrutura

---

## Arquitetura

```
src/pdet_fetcher/
├── __init__.py        # Re-exports da API pública
├── cli.py             # CLI nativa (sync / list / convert / columns / pipeline)
├── __main__.py        # Entry point para `python -m pdet_fetcher`
├── plugin.py          # Plugin Typer para quantilica-cli
├── fetch.py           # Conexão FTP, listagem e download
├── reader.py          # Leitura e tipagem dos CSVs
├── wrangling.py       # convert_rais, convert_caged, extract_columns_for_dataset
├── storage.py         # Convenções de caminhos no destino
├── constants.py       # Schemas de colunas, NA values, ragged files
└── meta.py            # Metadados (caminhos no FTP, padrões de filename)
```

### Fluxo de dados

```
FTP (ftp.mtps.gov.br)
    ↓  fetch_*
arquivos compactados (.7z, .zip)
    ↓  convert_* (chama 7z para extrair)
CSVs (latin-1 ou utf-8, separador detectado)
    ↓  read_rais / read_caged
DataFrame Polars com tipos corretos
    ↓  write_parquet
Parquet
```

---

## API pública

Todas estas funções são importáveis diretamente de `pdet_fetcher`:

| Função | Descrição |
|---|---|
| `connect()` | Conecta ao FTP da PDET (`ftp.mtps.gov.br`). |
| `list_rais(ftp)` / `list_rais_docs(ftp)` | Lista arquivos RAIS / docs. |
| `list_caged(ftp)` / `list_caged_docs(ftp)` | Lista CAGED clássico / docs. |
| `list_caged_2020(ftp)` / `list_caged_2020_docs(ftp)` | Lista Novo CAGED / docs. |
| `fetch_rais(ftp, dest_dir)` / `fetch_rais_docs(...)` | Baixa RAIS / docs. |
| `fetch_caged(ftp, dest_dir)` / `fetch_caged_docs(...)` | Baixa CAGED clássico / docs. |
| `fetch_caged_2020(ftp, dest_dir)` / `fetch_caged_2020_docs(...)` | Baixa Novo CAGED / docs. |
| `convert_rais(data_dir, dest_dir)` | Descompacta e grava RAIS em Parquet. |
| `convert_caged(data_dir, dest_dir)` | Descompacta e grava CAGED em Parquet. |
| `extract_columns_for_dataset(...)` | Extrai os nomes de coluna de cada arquivo do dataset. |

Funções de leitura de baixo nível em `pdet_fetcher.reader`:

- `read_rais(filepath, year, dataset, **read_csv_args)` — `dataset` ∈ `{"vinculos", "estabelecimentos"}`
- `read_caged(filepath, date, dataset, **read_csv_args)` — `dataset` ∈ `{"caged", "caged-ajustes", "caged-2020-mov", "caged-2020-for", "caged-2020-exc"}`
- `write_parquet(df, filepath)`
- `decompress(file_metadata)` — invoca o binário `7z` para extrair

---

## Exemplos de Análise

### 1. Qual setor criou mais empregos em 2023?

```python
import polars as pl
from pathlib import Path
from pdet_fetcher.reader import read_rais

df = read_rais(Path("rais_2023_vinculos.csv"), year=2023, dataset="vinculos")

top_setores = df.group_by("cnae_secao").agg(
    pl.col("id_vinculo").count().alias("empregos")
).sort("empregos", descending=True)

print(top_setores)
```

### 2. Evolução do emprego nos últimos 5 anos

```python
import polars as pl
from pdet_fetcher.reader import read_rais
from pathlib import Path

df = pl.concat([
    read_rais(Path(f"rais_{y}_vinculos.csv"), year=y, dataset="vinculos").with_columns(
        ano=pl.lit(y)
    )
    for y in range(2019, 2024)
])

evolucao = df.group_by("ano").agg(
    empregos=pl.col("id_vinculo").count(),
    salario_medio=pl.col("vl_remun_medio_nominal").mean()
)

print(evolucao.sort("ano"))
```

### 3. Desempenho de mercado em tempo real (Novo CAGED)

```python
import polars as pl
from pathlib import Path
from pdet_fetcher.reader import read_caged

# Ler últimos 12 meses (movimentações no prazo)
arquivos = sorted(Path("dados").glob("cagedmov_*.csv"))[-12:]

df = pl.concat([
    read_caged(f, date=int(f.stem.split("_")[-1]), dataset="caged-2020-mov")
    for f in arquivos
])

saldo_mensal = df.with_columns(
    saldo=(pl.col("admissoes") - pl.col("demissoes"))
).group_by("competencia").agg(
    saldo_total=pl.col("saldo").sum()
).sort("competencia")

print(saldo_mensal)
```

### 4. Diferenças salariais por gênero e setor

```python
import polars as pl
from pdet_fetcher.reader import read_rais

df = read_rais(Path("rais_2023_vinculos.csv"), year=2023, dataset="vinculos")

diferenca = df.group_by(["cnae_secao", "ind_sexo_trabalhador"]).agg(
    salario_medio=pl.col("vl_remun_medio_nominal").mean(),
    qtd_pessoas=pl.col("id_vinculo").count()
).sort(["cnae_secao", "ind_sexo_trabalhador"])

print(diferenca)
```

---

## Tratamento de Dados

A ferramenta detecta e corrige automaticamente:

- **Valores faltantes**: Diferentes representações (espaços, pontos, valores nulos)
- **Formato de números**: Remove espaçamento e converte separadores decimais
- **Problemas de encoding**: Suporta latin-1 e utf-8
- **CSVs ragged**: Fixa linhas com número inconsistente de colunas
- **Tipos de dados**: Converte strings em INT64, FLOAT64, BOOLEAN conforme necessário

---

## Referências

- **PDET**: [pdet.mte.gov.br](http://pdet.mte.gov.br/microdados-rais-e-caged)
- **RAIS**: [Documentação Oficial](http://pdet.mte.gov.br/rais)
- **CAGED**: [Documentação Oficial](http://pdet.mte.gov.br/caged)
- **Novo CAGED (2020+)**: [Documentação Oficial](http://pdet.mte.gov.br/novo-caged)

---

## Desenvolvimento

```bash
git clone https://github.com/Quantilica/pdet-fetcher.git
cd pdet-fetcher
uv sync --dev
uv run pytest
```

## Licença

MIT — veja [LICENSE](LICENSE).

---

**Última atualização**: Abril de 2026
