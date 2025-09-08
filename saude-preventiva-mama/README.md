# Projeto de Extração de Dados BI-RADS

Este projeto tem como objetivo a extração e manipulação de dados relacionados ao BI-RADS (Breast Imaging Reporting and Data System), um sistema utilizado para categorizar resultados de exames de imagem da mama. O projeto é dividido em dois notebooks Jupyter, cada um com suas respectivas funcionalidades e propósitos.

## Estrutura do Projeto

O projeto contém os seguintes arquivos:

- **src/1_pardini_mama_birads.ipynb**: Este é o notebook original que contém a implementação para a extração de dados relacionados ao BI-RADS. Ele inclui células de código que instalam pacotes, realizam consultas em bancos de dados e transformam dados, além de funções para manipulação e salvamento de dados.

- **src/1_pardini_mama_birads_revisado.ipynb**: Este é o novo notebook gerado, que mantém a mesma estrutura de código do arquivo original, mas inclui uma célula Markdown no início que fornece uma explicação abrangente sobre o notebook, detalhando a implementação, a sequência de execução, as funções utilizadas e as tabelas envolvidas. Além disso, cada célula de código subsequente é precedida por uma célula Markdown que explica detalhadamente o que o código faz.

## Instruções de Uso

1. **Instalação de Dependências**: Certifique-se de que todas as bibliotecas necessárias estão instaladas. O notebook original inclui uma célula que instala o pacote `octoops`.

2. **Execução do Notebook**: Abra o notebook desejado e execute as células na ordem apresentada. O notebook revisado é recomendado para uma melhor compreensão do código, pois contém explicações detalhadas.

3. **Consultas e Transformações**: Os notebooks realizam consultas em um banco de dados específico e transformam os dados extraídos para análise posterior. As funções implementadas são responsáveis por manipular os dados e salvá-los em tabelas Delta.

## Tabelas Envolvidas

- **refined.saude_preventiva.pardini_laudos_mama_birads**: Tabela que armazena os laudos de exames de mama com informações sobre o BI-RADS.

- **refined.saude_preventiva.pardini_laudos_mama_birads_ativacao**: Tabela que contém informações sobre a ativação de pacientes com base nos resultados do BI-RADS.

Este projeto visa facilitar a análise e o acompanhamento de pacientes com exames de mama, contribuindo para a melhoria da saúde preventiva.