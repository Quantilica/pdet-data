CREATE TABLE IF NOT EXISTS pdet.rais_vinculos (
    ano INTEGER,
    cnae_1 TEXT,
    valor_remuneracao_junho NUMERIC,
    idade INTEGER,
    cnae_2 TEXT,
    faixa_horas_contratadas TEXT,
    cnae_2_subclasse TEXT,
    valor_remuneracao_abril NUMERIC,
    tipo_vinculo TEXT,
    valor_remuneracao_outubro NUMERIC,
    vinculo_ativo_3112 BOOLEAN,
    quantidade_dias_afastamento INTEGER,
    indicador_simples BOOLEAN,
    subatividade_ibge TEXT,
    bairros_sp TEXT,
    indicador_trabalho_parcial BOOLEAN,
    quantidade_horas_contratadas INTEGER,
    valor_remuneracao_marco NUMERIC,
    raca_cor TEXT,
    valor_remuneracao_novembro NUMERIC,
    tipo_deficiencia TEXT,
    ano_chegada_brasil INTEGER,
    tempo_emprego NUMERIC,
    valor_remuneracao_dezembro_sm NUMERIC,
    valor_remuneracao_setembro NUMERIC,
    causa_desligamento_1 TEXT,
    faixa_etaria TEXT,
    tamanho_estabelecimento TEXT,
    cbo_2002 TEXT,
    sexo TEXT,
    tipo_estabelecimento_desc TEXT,
    causa_desligamento_3 TEXT,
    tipo_salario TEXT,
    grau_instrucao_1985_2005 TEXT,
    bairros_fortaleza TEXT,
    faixa_remuneracao_dezembro_sm TEXT,
    indicador_portador_deficiencia BOOLEAN,
    valor_remuneracao_media_sm NUMERIC,
    cbo_1994 TEXT,
    motivo_desligamento TEXT,
    indicador_cei_vinculado BOOLEAN,
    valor_remuneracao_dezembro NUMERIC,
    distritos_sp TEXT,
    subsetor_ibge TEXT,
    regioes_administrativas_df TEXT,
    grau_instrucao_apos_2005 TEXT,
    causa_desligamento_2 TEXT,
    valor_remuneracao_agosto NUMERIC,
    mes_desligamento INTEGER,
    valor_salario_contratual TEXT,
    indicador_trabalho_intermitente BOOLEAN,
    id_municipio_6 TEXT,
    nacionalidade TEXT,
    tipo_admissao TEXT,
    id_municipio_6_trabalho TEXT,
    natureza_juridica TEXT,
    valor_remuneracao_janeiro NUMERIC,
    mes_admissao INTEGER,
    valor_remuneracao_fevereiro NUMERIC,
    valor_remuneracao_julho NUMERIC,
    faixa_remuneracao_media_sm TEXT,
    faixa_tempo_emprego TEXT,
    tipo_estabelecimento TEXT,
    bairros_rj TEXT,
    valor_remuneracao_maio NUMERIC,
    valor_remuneracao_media NUMERIC
) PARTITION BY RANGE (ano);


CREATE INDEX idx_rais_vinculos_ano ON pdet.rais_vinculos(ano);
CREATE INDEX idx_rais_vinculos_cnae_1 ON pdet.rais_vinculos(cnae_1);
CREATE INDEX idx_rais_vinculos_cnae_2 ON pdet.rais_vinculos(cnae_2);
CREATE INDEX idx_rais_vinculos_cnae_2_subclasse ON pdet.rais_vinculos(cnae_2_subclasse);
CREATE INDEX idx_rais_vinculos_cbo_2002 ON pdet.rais_vinculos(cbo_2002);
CREATE INDEX idx_rais_vinculos_id_municipio_6 ON pdet.rais_vinculos(id_municipio_6);
CREATE INDEX idx_rais_vinculos_id_municipio_6_trabalho ON pdet.rais_vinculos(id_municipio_6_trabalho);
CREATE INDEX idx_rais_vinculos_vinculo_ativo_3112 ON pdet.rais_vinculos(vinculo_ativo_3112);


DO $$
DECLARE
    start_year INTEGER := 1985;
    end_year INTEGER := 2024;
    current_year INTEGER;
    partition_name TEXT;
BEGIN
    FOR current_year IN start_year..end_year LOOP
        partition_name := 'pdet.rais_vinculos_' || current_year;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF pdet.rais_vinculos FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            current_year,
            current_year + 1
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END LOOP;
END$$;
