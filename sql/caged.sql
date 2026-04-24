CREATE TABLE IF NOT EXISTS pdet.caged (
    -- Identificadores geográficos
    id_municipio_6 TEXT,
    id_uf TEXT,
    id_mesorregiao TEXT,
    id_microrregiao TEXT,
    regiao_corede TEXT,
    regiao_corede_04 TEXT,
    regiao_senac_pr TEXT,
    regiao_senai_pr TEXT,
    regiao_senai_sp TEXT,
    regiao_adm_rj TEXT,
    regiao_adm_sp TEXT,
    regiao_gov_sp TEXT,
    regioes_adm_df TEXT,
    distritos_sp TEXT,
    bairros_sp TEXT,
    bairros_fortaleza TEXT,
    bairros_rj TEXT,

    -- Informações do trabalhador
    idade INTEGER,
    sexo TEXT,
    raca_cor TEXT,
    grau_instrucao TEXT,
    tipo_defic TEXT,
    indicador_portador_deficiencia BOOLEAN,

    -- Informações do emprego
    cbo_2002_ocupacao TEXT,
    cnae_1 TEXT,
    cnae_2 TEXT,
    cnae_2_subclasse TEXT,
    tipo_estab TEXT,
    tamanho_estabelecimento_janeiro TEXT,
    quantidade_horas_contratadas INTEGER,
    salario_mensal TEXT,
    tempo_emprego NUMERIC,
    indicador_trabalho_parcial BOOLEAN,
    indicador_trabalho_intermitente BOOLEAN,
    indicador_aprendiz BOOLEAN,

    -- Movimentações
    tipo_mov_desagregado TEXT,
    admitidos_desligados TEXT,
    saldo_mov TEXT,
    competencia_movimentacao TEXT,
    ano_movimentacao TEXT,

    -- Outros
    subsetor_ibge TEXT,
    subregiao_senai_pr TEXT
) PARTITION BY LIST (ano_movimentacao);