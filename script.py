import apache_beam as beam

header = 'Data;Estado;UF;QtdVendas;QtdCancelamentos;QtdAprovados'
columns = ['Data','Estado','UF','QtdVendas','QtdCancelamentos','QtdAprovados']
states = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AP': 'Amapa',
    'AM': 'Amazonas',
    'BA': 'Bahia',
    'CE': 'Ceara',
    'DF': 'Distrito Federal',
    'ES': 'Espirito Santo',
    'GO': 'Goias',
    'MA': 'Maranhao',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Para',
    'PB': 'Paraiba',
    'PR': 'Parana',
    'PE': 'Pernambuco',
    'PI': 'Piaui',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondonia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'Sao Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins'
}

class CreateOutputFields(beam.DoFn):
    def __init__(self):
        return
    def process(self, line):
        line[0] = line[1]
        line[1] = states[line[2]]
        status = line[3]
        line[3] = 1
        if (status == 'Aprovado'):    
            line.append(0)
            line.append(1)
        else:
            line.append(1)
            line.append(0)
        yield line

class SumQtds(beam.DoFn):
    def __init__(self):
        return
    def process(self, tup):
        values = tup[1] # list of lists
        qtdVendas = 0
        qtdCancelados = 0
        qtdAprovados = 0
        for value in values:
            qtdVendas += value[3]
            qtdCancelados += value[4]
            qtdAprovados += value[5]
        values = [values[0][0], values[0][1], values[0][2], qtdVendas, qtdCancelados, qtdAprovados]
        convertedTuple = list(tup)
        convertedTuple[1] = values
        tup = tuple(convertedTuple)
        yield tup

with beam.Pipeline() as p1:
    #linesIBGE = pipeline | 'ReadIBGEData' >> beam.io.ReadFromText('data/EstadosIBGE.csv')
    groupedData = (
        p1
        | 'Read Vendas file' >> beam.io.ReadFromText('data/Vendas_por_dia.csv', skip_header_lines=True) 
        | 'Split ;' >> beam.Map(lambda x: x.split(';')) 
        | beam.ParDo(CreateOutputFields())
        | 'Group by UF and Date' >> beam.GroupBy(lambda s: s[0]+s[1])
        | beam.ParDo(SumQtds())
        | 'Values' >> beam.Values()
        | beam.Map(lambda s: s[0] + ';' + s[1] + ';' + s[2] + ';' + str(s[3]) + ';' + str(s[4]) + ';' + str(s[5]))
        | beam.io.WriteToText('output', file_name_suffix='.csv', header=header)
        | beam.Map(print)
    )

with beam.Pipeline() as p2:
    outputJSON = (
        p2
        | 'Read Grouped Data file' >> beam.io.ReadFromText('output-00000-of-00001.csv', skip_header_lines=True) 
        | 'Split ; again' >> beam.Map(lambda x: x.split(';')) 
        | 'Map to JSON' >> beam.Map(lambda s: '{ "Data":"' + s[0] + '","Estado":"' + s[1] + '","UF":"' + s[2] + '","QtdVendas":' + str(s[3]) + ',"QtdCancelamentos":' + str(s[4]) + ',"QtdAprovados":' + str(s[5]) + '},')
        | beam.io.WriteToText('output', file_name_suffix='.json')
        | 'Print the JSON file' >> beam.Map(print)
    )
