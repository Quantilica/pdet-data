CREATE TABLE IF NOT EXISTS pdet.rais_estabelecimentos (
    ano INTEGER,
    cep TEXT,
    bairros_sp TEXT,
    indicador_pat TEXT,
    subsetor_ibge TEXT,
    distritos_sp TEXT,
    tamanho TEXT,
    subatividade_ibge TEXT,
    indicador_atividade_ano TEXT,
    tipo_desc TEXT,
    quantidade_vinculos_clt INTEGER,
    bairros_rj TEXT,
    natureza TEXT,
    indicador_simples BOOLEAN,
    natureza_juridica TEXT,
    indicador_rais_negativa TEXT,
    sigla_uf TEXT,
    cnae_2_subclasse TEXT,
    quantidade_vinculos_ativos INTEGER,
    cnae_2 TEXT,
    id_municipio_6 TEXT,
    bairros_fortaleza TEXT,
    quantidade_vinculos_estatutarios INTEGER,
    regioes_administrativas_df TEXT,
    cnae_1 TEXT,
    indicador_cei_vinculado BOOLEAN,
    tipo TEXT,
    id_uf TEXT
) PARTITION BY RANGE (ano);

CREATE INDEX idx_rais_estabelecimentos_ano ON pdet.rais_estabelecimentos(ano);
CREATE INDEX idx_rais_estabelecimentos_id_uf ON pdet.rais_estabelecimentos(id_uf);
CREATE INDEX idx_rais_estabelecimentos_sigla_uf ON pdet.rais_estabelecimentos(sigla_uf);
CREATE INDEX idx_rais_estabelecimentos_id_municipio_6 ON pdet.rais_estabelecimentos(id_municipio_6);
CREATE INDEX idx_rais_estabelecimentos_cnae_1 ON pdet.rais_estabelecimentos(cnae_1);
CREATE INDEX idx_rais_estabelecimentos_cnae_2 ON pdet.rais_estabelecimentos(cnae_2);
CREATE INDEX idx_rais_estabelecimentos_cnae_2_subclasse ON pdet.rais_estabelecimentos(cnae_2_subclasse);
CREATE INDEX idx_rais_estabelecimentos_tipo ON pdet.rais_estabelecimentos(tipo);
CREATE INDEX idx_rais_estabelecimentos_tipo_desc ON pdet.rais_estabelecimentos(tipo_desc);
CREATE INDEX idx_rais_estabelecimentos_subsetor_ibge ON pdet.rais_estabelecimentos(subsetor_ibge);
CREATE INDEX idx_rais_estabelecimentos_subatividade_ibge ON pdet.rais_estabelecimentos(subatividade_ibge);
CREATE INDEX idx_rais_estabelecimentos_natureza_juridica ON pdet.rais_estabelecimentos(natureza_juridica);
CREATE INDEX idx_rais_estabelecimentos_indicador_atividade_ano ON pdet.rais_estabelecimentos(indicador_atividade_ano);

-- Create partitions for each year from 1985 to 2024
-- and sub-partitions for each UF
DO $$
DECLARE
    start_year INTEGER := 1985;
    end_year INTEGER := 2024;
    current_year INTEGER;
    partition_name TEXT;
BEGIN
    FOR current_year IN start_year..end_year LOOP
        partition_name := 'pdet.rais_estabelecimentos_' || current_year;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF pdet.rais_estabelecimentos FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            current_year,
            current_year + 1
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END LOOP;
END$$;
