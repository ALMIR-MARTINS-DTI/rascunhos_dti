# PROMPT PARA INSERIR CÉLULAR MARKDOWN NOS NOTEBOOKS 
### É CRIADO UM NOVO COM AS CÉLULAS DE CÓDIGO ACRESCENTANDO AS CÉLULAS MARKDOWN ==

Sua tarefa é criar uma versão documentada deste notebook. Por favor, siga estas instruções detalhadas:

1.  **Crie um novo notebook:** Crie um novo notebook Jupyter (`.ipynb`) no mesmo diretório do notebook original, com o nome `[NOME_DO_NOTEBOOK_ORIGINAL]_editado.ipynb`. Não crie novas pastas nem altere a estrutura de diretórios, apenas crie o notebook dentro da pasta atual. Se tiver dúvidas sobre a pasta atual, peça para apontar qual é a pasta. 
2.  **Copie o conteúdo das células de código:** Transfira *todas* as células de código do notebook original (`[NOME_DO_NOTEBOOK_ORIGINAL].ipynb`) para o novo notebook (`[NOME_DO_NOTEBOOK_ORIGINAL]_editado.ipynb`), mantendo a ordem e o conteúdo exatos, sem nenhuma alteração no código.
3.  **Adicione uma Introdução Detalhada (Célula Markdown Inicial):**
*   No início do novo notebook, insira uma nova célula Markdown.
*   Nesta célula, gere uma introdução técnica e detalhada sobre todo o notebook.
*   A introdução deve ser organizada com tópicos, destaques (negrito, itálico), exemplos de código (se aplicável, para sintaxe ou nomes de colunas/variáveis), e deve mencionar nomes de tabelas, recursos, bibliotecas e frameworks utilizados.
*   Deve cobrir:
    *   **Objetivo Principal:** Qual problema este notebook resolve ou qual tarefa ele executa?
    *   **Tecnologias Utilizadas:** Principais bibliotecas Python (pandas, numpy, scikit-learn, matplotlib, etc.), frameworks ou ferramentas externas.
    *   **Fluxo de Trabalho/Etapas Principais:** Sequência lógica das operações (carregamento de dados, pré-processamento, análise exploratória, modelagem, visualização, salvamento).
    *   **Dados Envolvidos:** Fontes de dados, nomes de tabelas/arquivos, colunas importantes, formato dos dados.
    *   **Resultados/Saídas Esperadas:** O que este notebook produz como resultado final.
    *   **Pré-requisitos:** Requisitos para execução (ambiente Python, pacotes, acesso a dados).
    *   **Considerações Importantes/Observações:** Detalhes técnicos relevantes, suposições, limitações.
4.  **Insira Explicações Detalhadas (Células Markdown Antes do Código):**
*   Para *cada* célula de código copiada, insira uma nova célula Markdown *imediatamente antes* dela.
*   Nesta nova célula Markdown, gere uma explicação técnica e detalhada do que a célula de código subsequente faz.
*   A explicação não deve ser superficial; deve ser proporcional à profundidade do código.
*   Deve cobrir:
    *   **Objetivo da Célula:** O que esta célula faz?
    *   **Dependências:** Quais bibliotecas, funções ou dados de células anteriores são utilizados?
    *   **Variáveis/Objetos Criados/Modificados:** Quais variáveis ou objetos são definidos ou alterados?
    *   **Lógica Detalhada:** Explique o passo a passo da lógica, incluindo transformações de dados, cálculos, chamadas de API, etc.
    *   **Nomes de Features/Colunas/Tabelas:** Mencione explicitamente quaisquer nomes de colunas, features, tabelas ou arquivos que são manipulados.
    *   **Saída/Impacto:** Qual é o resultado imediato da execução desta célula? Como ela afeta o estado do notebook ou os dados?
    *   **Exemplos (se aplicável):** Se houver um trecho de código ou sintaxe específica que mereça destaque, inclua-o em um bloco de código Markdown.

4.  **Insira Explicações Detalhadas (Células Markdown Antes do Código):**
*   Para *cada* célula de código copiada, insira uma nova célula Markdown *imediatamente antes* dela.
*   Nesta nova célula Markdown, gere uma explicação técnica e detalhada do que a célula de código subsequente faz.
*   **A profundidade e a estrutura da explicação devem ser proporcionais à complexidade do código na célula.**
    *   **Para células de código complexas ou que realizam múltiplas operações:** Utilize uma estrutura detalhada com tópicos como: 
  
        *   **Objetivo da Célula:** O que esta célula faz?
        *   **Dependências:** Quais bibliotecas, funções ou dados de células anteriores são utilizados?
        *   **Variáveis/Objetos Criados/Modificados:** Quais variáveis ou objetos são definidos ou alterados?
        *   **Lógica Detalhada:** Explique o passo a passo da lógica, incluindo transformações de dados, cálculos, chamadas de API, etc.
        *   **Nomes de Features/Colunas/Tabelas:** Mencione explicitamente quaisquer nomes de colunas, features, tabelas ou arquivos que são manipulados.
        *   **Saída/Impacto:** Qual é o resultado imediato da execução desta célula? Como ela afeta o estado do notebook ou os dados?
        *   **Exemplos (se aplicável):** Se houver um trecho de código ou sintaxe específica que mereça destaque, inclua-o em um bloco de código Markdown.
    
    *   **Para células de código simples (ex: um único `print()`, uma importação simples, uma atribuição de variável trivial):** Forneça uma explicação concisa, em um ou dois parágrafos, sem a necessidade de tópicos explícitos, focando diretamente no objetivo e impacto daquela linha ou poucas linhas de código.
   

**Atenção:**
*   Não crie novos diretórios ou pastas.
*   Não altere *nenhum* código das células originais.
*   Todas as explicações devem ser em Markdown, utilizando formatação rica (títulos, listas, negrito, blocos de código para exemplos de sintaxe ou nomes de colunas/tabelas).
*   Garanta que a profundidade da explicação Markdown seja compatível com a complexidade do código que ela descreve.

**Confirmação:** Após a conclusão, por favor, confirme que o novo notebook foi criado e que todas as células de código foram copiadas e devidamente documentadas com as células Markdown antes de cada uma, além da introdução geral.

