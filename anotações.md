Curso de Eng. de Dados com Apache Beam
==

Passos realizados:

1 - Instalação e configuração do Ambiente Python e do Apache Beam
2 - Tratamentos iniciais dos dados (criação do pipeline, conversão de strings para lista, conversão de listas para dicionário)
3 - Um pouco sobre como executar como expressões lambda
4 - tratamento de datas
5 - tratamento por estados
6 - Agora agrupado por estado, é possível fazer os tratamentos por ano/mês, além de verificar a quantidade de casos pelo Estado
Em datasets diferentes, utilizar as informações que se tem em comum
7 - fazer mais alguns ajustes na normalização e tratar os dados para análise
8 - transformar a tupla acrescentando a chave de ano/mês ao estado e remover os campos que não serão utilizados (pra cada elemento do array, retorna uma tupla contendo a chave do agrupamento e a quantidade de casos)
9 - descompactar os dados
10 - iniciar primeiras análises (somar os casos de chaves iguais e retornar chasves únicas com o valor total da soma das chaves)
11 - Criar chave como no processo de dengue
12 - fazer o agrupamento e a soma (como no processo de dengue), retornando chaves únicas com o valor total da soma das chaves
13 - fazer os arrendondamentos necessários
14 - Agrupar pela chave para unir em uma única tupla (união)
15 - Fazer manutenção nos gargalos dos datasets
16 - pcollection de chuva e dengue
17 - Juntar as pcollections em uma única (Tuplas de mesma chave, agrupa-las pela chave)
18 - Explicitar os valores e cortando caminhos
19 - Tratar campos vazios nos resultado (aplicar um filtro para remover esses casos)
20 - Descompactar a chave (composta por três elementos BA-2020-12) e tirar os valores dentro do dicionário
21 - Preparação para escrita e persistência dos resultados (ex. csv, transformar a tupla em uma string separada por vírgula)
22 - Escrita no Jupyter notebook para análise dos resultados

Data, casos, cidade-estado, / data, quantidade chuvas, estado

Golden Circle - O que, porque e como

Remover algumas colunas e outras readequar (normalizar)

Problema bem comum na engenharia de dados > tempo de processamento
Engenharia de dados > Arquivos grandes

Apache Beam
Leitura linha a linha do arquivo, trata cada linha individualmente
Paralelização dos processos em múltiplos processos simultâneos

O tempo total do processamento será dividido em processos simultâneos em vCPU's

Estrutura da Pipeline no Apache Beam

Pcollection vai guardar o estado da etapa em trabalho com a aplicação de processos definidos na SDK;

Leitura de um arquivo (Texto, arquivos semi estruturados (avro, parquet, BigQuery)

SDK
	ReadFromText: Arquivo de formato texto (local do arquivo, parâmetros (ex.: pular cabeçalho do arquivo)

Criar algoritmos que facilitam o acesso aos dados brutos

Criar um método que receba a string e converta em lista, a partir do delimitador

Resultado da pcollection são strings
- Transforma esse resultado para facilitar transformações
- Identifica cada coluna lida

Atualizar dicionário
dict.update({'b': 2})
dict['c'] = 3
consultar dicionário
dict['a']
sem erros:
dict.get('d')

Unificar dados por Ano e mês
Criar um campo composto por esses valores (hash)
{'data_iniSE': '2016-08-21'}
{ano_mes: '2016-08'}

Normalização necessária para rodar as análises

Pra cada dicionário, retorna uma chave, da qual diz respeito ao Estado. Criar métodos que faça agrupamentos pelo Estado

GroupByKey:
Retorna um agrupamento em uma lista, contendo a chave que foi buscada em todos os elementos. Ou seja, se haviam 3 chaves iguais, ele retorna apenas 1, com uma lista contendo os demais elementos que antes estavam como estado da pcollection. Caso queira saber mais https://beam.apache.org/documentation/transforms/python/aggregation/groupbykey/

yield
- iteradores, geradores e expressões geradoras
- utilizado dentro de funções, pode retornar vários valores de uma mesma função

Os resultados da pcollection são listas;
Criar chave como no processo de dengue
Chave e total de chuvas e de dengue
Agrupar pela chave para unir em uma única tupla

pcollection de chuva e dengue
juntar as pcollections em uma única (agrupados pela chave com valores em um dicionário)



**Pegar arquivo sobre FlatMap**

Links úteis:
https://beam.apache.org/documentation/programming-guide/
https://beam.apache.org/documentation/transforms/python/elementwise/map/
https://beam.apache.org/documentation/patterns/pipeline-options/
https://beam.apache.org/documentation/transforms/python/aggregation/groupbykey/ - retorna uma tupla contendo o elemento chave e uma lista com os elementos que têm a mesma chave no processo que está inserido

https://beam.apache.org/documentation/transforms/python/aggregation/ - outros métodos de agregação
https://beam.apache.org/documentation/transforms/python/elementwise/flatmap/
https://book.pythontips.com/en/latest/generators.html
https://beam.apache.org/releases/pydoc/2.2.0/apache_beam.io.textio.html
https://beam.apache.org/documentation/transforms/python/other/flatten/ - apenas empilha as n pcols, sem realizar agrupamentos
https://beam.apache.org/documentation/transforms/python/aggregation/cogroupbykey/
https://beam.apache.org/documentation/transforms/python/elementwise/pardo/ - Pardo para melhorar pipeline de dados

https://www.google.com/search?client=firefox-b-e&q=cursor+multiplo+vscode

Cursos recomendados (para melhor aproveitar o Pardo):
Introdução a Orientação a objetos e Avançando na orientação a objetos com Python.

Ingestão de dados (ReadFromText)
Transformação de dados (Map, FlatMap, Filter, GroupByKey, CoGroupByKey, CombinePerKey)
Persistência de dados (WriteToText)
