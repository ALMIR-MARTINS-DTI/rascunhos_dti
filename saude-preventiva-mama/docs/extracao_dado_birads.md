# Extração de dado - BI_RADS

## Objetivo
- Extrair BI-RADIS 0,1,2,3,4,5,6 e sem birads.
- Salvar **lista de ativação**: considera apenas BI_RADS 1, 2 e 3 e tem com objetivo identificar pacientes com exames em atraso para envio de notificação.

## Resumo do Código SQL/PySpark

Este código SQL, executado via PySpark (`spark.sql`), tem como objetivo principal processar dados de laudos médicos de mamografia para extrair, padronizar e enriquecer informações relacionadas à classificação BIRADS, combinando-as com outros dados do paciente e informações de retorno elegível.

### Tópicos Principais:

#### Tabela e filtros:
*   A consulta começa selecionando dados de uma tabela de laudos médicos (`refined.saude_preventiva.fleury_laudos`), que contém informações detalhadas sobre exames e enriququece com dados de retorno da tabela `refined.saude_preventiva.fleury_retorno_elegivel_ficha`.
*   Coluna com informações de BIRADS: `laudo_tratado` (texto do laudo médico).
*   Filtros:
    *   linha_cuidado = 'mama'
    *   sigla_exame IN ('MAMOG', 'MAMOGDIG', 'MAMOPROT', 'MAMOG3D')
    *   **where_clause:** _datestamp (`refined.saude_preventiva.fleury_laudos`) >= _datestamp (`refined.saude_preventiva.fleury_laudos_mama_birads`)
    *   **filtro_ativacao:**
        *    eleg.ficha IS NULL
        *    brd.BIRADS IN (1, 2, 3)
        *    flr.sigla_exame IN ('MAMOG', 'MAMOGDIG', 'MAMOPROT', 'MAMOG3D')
        *    UPPER(flr.sexo_cliente) = 'F'
        *    idade_cliente >= 40 AND idade_cliente < 76

#### Extração e Padronização BIRADS (`base` CTE):
*   **Limpeza de Texto:** Remove caracteres indesejados (`-`, `:`, `®`, `\xa0`) e converte o texto do laudo para maiúsculas.
*   **Extração de Conteúdo:** Utiliza `REGEXP_EXTRACT` para isolar seções relevantes do laudo (Avaliação, Conclusão, Impressão, Opinião).
*   **Extração de BIRADS Bruto:** Aplica `REGEXP_EXTRACT_ALL` para encontrar todos os valores BIRADS (numéricos ou romanos como I-VI) dentro do texto extraído.
*   **Categorização BIRADS (`CAT_BIRADS`):** Transforma os valores BIRADS brutos em inteiros padronizados (1 a 6), tratando algarismos romanos e filtrando valores inválidos.

#### Análise de BIRADS Extraído (`dados_birads` CTE):
*   Calcula o valor **mínimo (`MIN_BIRADS`)** e **máximo (`MAX_BIRADS`)** das categorias BIRADS encontradas em um laudo.
*   Identifica o **último valor BIRADS (`BIRADS`)** presente no array de categorias.

*   **Seleção de Dados de Laudos (`dados_laudos` CTE):**
*   Seleciona diversas colunas de identificação e características dos laudos (`linha_cuidado`, `id_unidade`, `ficha`, `sigla_exame`, etc.).
*   Calcula a **idade do cliente (`idade_cliente`)** com base na data de nascimento e na data atual.
*   Inclui um placeholder `{where_clause}` para filtros dinâmicos na seleção dos laudos.

#### Consulta Principal (`SELECT` final):
*   **Junção de Dados:** Realiza um `INNER JOIN` entre os dados dos laudos (`dados_laudos`) e as informações BIRADS processadas (`dados_birads`) usando `ficha`, `id_item` e `id_subitem` como chaves.
*   **Enriquecimento com Retorno Elegível:** Executa um `LEFT JOIN` com a tabela `refined.saude_preventiva.fleury_retorno_elegivel_ficha` para adicionar detalhes sobre possíveis retornos ou acompanhamentos elegíveis para o paciente.
*   **Seleção de Colunas:** Seleciona todas as colunas dos laudos (exceto `idade_cliente`), as colunas BIRADS (`MIN_BIRADS`, `MAX_BIRADS`, `BIRADS`), e as colunas de retorno elegível.
*   Inclui um placeholder `{filtro_ativacao}` para filtros dinâmicos adicionais.

#### Fluxo de Execução do Notebook

O notebook segue o seguinte fluxo de processamento:

1.  **Definição de Variáveis e Filtros:** `table_birads`, `table_birads_ativacao`, `where_clause` (com lógica incremental) e `filtro_ativacao` são definidos.
2.  **Execução da Query SQL Principal:**
    *   A `query` SQL é executada duas vezes usando `spark.sql`:
        *   `df_spk`: Gerado com a `where_clause` incremental e `filtro_ativacao` vazio. Contém todos os laudos de mamografia com BIRADS extraídos e enriquecidos.
        *   `df_spk_ativacao`: Gerado com `where_clause` vazio e o `filtro_ativacao` específico para a lista de ativação.
3.  **Transformações nos DataFrames:**
    *   A função `transform_fields` é aplicada a `df_spk` e `df_spk_ativacao`. Esta função adiciona colunas como `retorno_cliente`, `dth_previsao_retorno` e `dias_ate_retorno`.
4.  **Desduplicação da Lista de Ativação:**
    *   `df_spk_ativacao` é desduplicado pela coluna `ficha` (`dropDuplicates(['ficha'])`) para garantir uma única entrada por paciente para fins de notificação.
5.  **Persistência dos Dados:**
    *   `save_data(df_spk, table_birads)`: Salva o DataFrame completo de BIRADS na tabela `table_birads`, realizando um `MERGE` se a tabela já existir (para atualizações incrementais) ou um `INSERT` se for a primeira vez.
    *   `insert_data(df_spk_ativacao, table_birads_ativacao)`: Salva a lista de ativação na tabela `table_birads_ativacao`, sempre sobrescrevendo o conteúdo existente para refletir a lista mais recente de pacientes elegíveis para notificação.
