import apache_beam as beam
from datetime import datetime
import json

header = 'Data;Estado;UF;QtdVendas;QtdCancelamentos;QtdAprovados'

# datetime to name the file
dt = datetime.now()
str_dt = dt.strftime("%d-%m-%Y,%H-%M-%S").split(',')[1]
print(str_dt)

# DoFunction to create the requested columns for output
class CreateOutputFields(beam.DoFn):
    def __init__(self):
        return
    def process(self, line):
        line[0] = line[1]
        line[1] = ''
        status = line[4]
        code = line[3]
        line[3] = 1
        if (status == 'Aprovado'):    
            line[4] = 0
            line.append(1)
        else:
            line[4] = 1
            line.append(0)
        yield [code] + line

# DoFunction to sum the QtdVendas, QtdCancelamentos and QtdAprovados for each Data and State
class SumQtds(beam.DoFn):
    def __init__(self):
        return
    def process(self, tup):
        values = tup[1] # list of lists
        qtdVendas = 0
        qtdCancelados = 0
        qtdAprovados = 0
        for value in values:
            qtdVendas += value[4]
            qtdCancelados += value[5]
            qtdAprovados += value[6]
        values = [values[0][0], values[0][1], values[0][2], values[0][3], qtdVendas, qtdCancelados, qtdAprovados]
        convertedTuple = list(tup)
        convertedTuple[1] = values
        tup = tuple(convertedTuple)
        yield tup

# DoFunction to add all objects in a single list
class AddToList(beam.DoFn):
    def __init__(self):
        self.window = beam.transforms.window.GlobalWindow()
    def process(self, obj):
        convertedDict = json.loads(obj)
        self.jsonList.append(convertedDict)
    def start_bundle(self):
        self.jsonList = []
    def finish_bundle(self):
        yield beam.utils.windowed_value.WindowedValue(
        value= self.jsonList,
        timestamp=0,
        windows=[self.window],
    )

# DoFunction to replace the empty string with the respective state name and return each data separately
class ReplaceStateName(beam.DoFn):
    def __init__(self):
        self.window = beam.transforms.window.GlobalWindow()
    def process(self, l):
        state = l[0][0]
        if len(l[1]) != 0:
            for i in range(0,len(l[1][0])):
                l[1][0][i][2] = state
                yield l[1][0][i][1:]


with beam.Pipeline() as pipeline:
    # Read and format IBGE file
    ibgeData = (
        pipeline
        | 'Read IBGE file' >> beam.io.ReadFromText('data/EstadosIBGE.csv', skip_header_lines=True) 
        | 'Split the IBGE data by ;' >> beam.Map(lambda x: x.split(';'))
        | 'Select useful columns' >>  beam.Map(lambda s: tuple([s[1], str(s[0])]))
    )

    # Read and format Vendas file
    vendasData = (
        pipeline
        | 'Read Vendas file' >> beam.io.ReadFromText('data/Vendas_por_dia.csv', skip_header_lines=True) 
        | 'Split the Vendas data by ;' >> beam.Map(lambda x: x.split(';')) 
        | 'Create the requested columns for output' >> beam.ParDo(CreateOutputFields())
        | 'Group by UF and Date' >> beam.GroupBy(lambda s: s[0]+s[1])
        | 'Add the amounts for each group' >> beam.ParDo(SumQtds())
        | 'Get grouped Vendas values' >> beam.Values()
        | 'Group by UF Code' >> beam.GroupBy(lambda s: s[0]) # line necessary to merge with IBGEdata
    )

    # Merge the IBGE and Vendas data to relate the UF with the state name
    mergedData =  ((ibgeData, vendasData) | 'Merge PCollections' >> beam.CoGroupByKey())

    # Generate a PCollection with the output data
    outputData = (
        mergedData
        | 'Get mergedData values' >> beam.Values()
        | 'Replaces the Estado field with its name' >> beam.ParDo(ReplaceStateName())
    )

    # Format data and write to CSV file
    csvData = (
        outputData
        | 'Format data for CSV'>> beam.Map(lambda s: s[0] + ';' + s[1] + ';' + s[2] + ';' + str(s[3]) + ';' + str(s[4]) + ';' + str(s[5]))
        | 'Write CSV file' >> beam.io.WriteToText('output-' + str_dt, file_name_suffix='.csv', header=header)
    )

    # Format data and write to JSON file
    jsonData = (
        outputData
        | 'Format data for objects' >> beam.Map(lambda s: '{ "Data":"' + s[0] + '","Estado":"' + s[1] + '","UF":"' + s[2] + '","QtdVendas":' + str(s[3]) + ',"QtdCancelamentos":' + str(s[4]) + ',"QtdAprovados":' + str(s[5]) + '}')
        | 'Add formatted objects to the list' >> beam.ParDo(AddToList())
        | 'Fix double quotes' >>  beam.Map(json.dumps)
        | 'Write JSON file' >> beam.io.WriteToText('output-' + str_dt, file_name_suffix='.json')
    )