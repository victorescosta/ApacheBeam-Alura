import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

#building a pipeline
pipeline_options = PipelineOptions(argv=None)
#instanciando a partir da classe beam
#posteriormente as opções serão inseridas como: quantidade de máquinas, memória
pipeline = beam.Pipeline(options=pipeline_options)

# column names to transform list to dict
colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

#use split function to separate elements
def text_to_list(element, delimiter='|'):
    """
    Receives a text and a delimiter
    Return a list of elements by the delimiter
    """
    return element.split(delimiter)

#use zip function and apply dict into it
def list_to_dict(element, column):
    """
    Receives 2 lists
    Returns a dictionary
    """
    return dict(zip(column, element))

#date treatment. don't receive another parameter in .Map function
def date_treatment(element):
    """
    Receives a dict and creates a new field with YEAR-MONTH
    Returns the same dictionary with new field
    """
    element['year_month'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

#uf treatment
def uf_key(element):
    """
    Receives a dict
    Returns a tuple with UF and an element (UF, dict)
    """
    key = element['uf']
    return (key, element)

#dengue cases
def dengue_cases(element):
    """
    Receives a tuple ('BA', [{},{}])
    Returns a tuple ('BA-2014-12', 8.0)
    """
    uf, registros = element
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['year_month']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['year_month']}", 0.0)

#converts a list to a tuple
def list_key_uf_yearmonth(element):
    """
    Receives a list of elements
    Returns a tuple with a key and a value of chuva in mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = element
    ano_mes = '-'.join(data.split('-')[:2])
    key = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return key, mm

#round results
def round_results(element):
    """
    Receives a tuple ('PA-2019-06', 2364.000000004)
    Returns a tuple with its rounded value ('PA-2019-06', 2364.0)
    """
    key, mm = element
    return (key, round(mm, 1))

#method to filter empty cases
def filter_empty_cases(element):
    """
    Remove elements contained empty keys
    Receives a tuple ('CE-2015-12', {'chuvas': [7.6], 'dengue': [29.0]})
    Returns the same tuple without empty keys
    """
    key, data = element
    if all([
        data['chuvas'],
        data['dengue']
    ]):
        return True
    return False

#decompress elements
def decompress_elements(elements):
    """
    Receives a tuple ('CE-2015-12', {'chuvas': [7.6], 'dengue': [29.0]})
    Returns a tuple ('CE', '2015', '11', '0.4', '21.0')
    """
    key, data = elements
    chuvas = data['chuvas'][0]
    dengue = data['dengue'][0]
    uf, year, month = key.split('-')
    return uf, year, month, str(chuvas), str(dengue)

def prepare_csv(element, delimiter=';'):
    """
    Receives a tuple ('CE',2015,11, 0.4, 21.0)
    Return a limited string "CE;2015;11;0.4;21.0"
    """
    return f"{delimiter}".join(element)

#pcollection dengue
dengue = (
    pipeline
    | "Dengue's dataset reading" >>
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    |"Text to list" >> beam.Map(text_to_list)
    |"List to dict" >> beam.Map(list_to_dict, colunas_dengue)
    |"Make field year_month" >> beam.Map(date_treatment)
    |"Make key by uf" >> beam.Map(uf_key)
    |"Group by state" >> beam.GroupByKey()
    |"Decompress dengue cases" >> beam.FlatMap(dengue_cases)
    |"Sum cases by key" >> beam.CombinePerKey(sum)
    #|"Show results" >> beam.Map(print)
)

#pcollection chuvas
chuvas = (
    pipeline
    | "Chuvas' dataset reading" >>
        ReadFromText('chuvas.csv', skip_header_lines=1)
    |"Text to chuvas' list" >> beam.Map(text_to_list, delimiter=',')
    |"Make a key UF-ANO-MES" >> beam.Map(list_key_uf_yearmonth)
    |"Sum chuva's total cases by key" >> beam.CombinePerKey(sum)
    |"Round chuva results" >> beam.Map(round_results)
    #|"Show chuvas' results" >> beam.Map(print)
)

result = (
    # (chuvas, dengue)
    # |"Pile up pcols" >> beam.Flatten()
    # |"Group pcols" >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue': dengue})
    |"Merge pcols" >> beam.CoGroupByKey()
    |"Filter empty values" >> beam.Filter(filter_empty_cases)
    |"Decompress elements" >> beam.Map(decompress_elements)
    |"Prepare csv" >> beam.Map (prepare_csv)
    #|"Show union results" >> beam.Map(print)
)
#building a header name
header = "UF;YEAR;MONTH;CHUVA;DENGUE"

#write results in a text file or any datatype
result | "Create a CSV file" >> WriteToText('result', file_name_suffix='.csv', header=header)

# lambda function
# dengue = (
#     pipeline
#     | "Leitura do dataset de dengue" >>
#         ReadFromText('sample_casos_dengue.txt',
# skip_header_lines=1)
#     |"Texto para lista" >> beam.Map(lambda x: x.split('|'))
# )

#run the pipeline
pipeline.run()



