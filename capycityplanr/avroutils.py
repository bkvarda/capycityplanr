import logging, avro.schema, csv, csvobject, json
from csvobject import CSVObject
from fastavro import writer
#Converts csv to avro
def csvToAvro(csvobj):
    avro_path = csvobj.config.temp_dir + '/' + csvobj.customer + '_' + csvobj.type + '.avro'
    logging.info('Converting CSV to Avro')
    logging.info('CSV Path: ' + csvobj.path + ', Avro Output Path: ' + avro_path)
    with open(avro_path, 'wb') as outfile:
        
        data = csvReader(csvobj)
        for thing in data:
            print(str(thing))
        writer(outfile, schema, data)
    csvobj.setAvro(avro_path)
    logging.info('Conversion of CSV to Avro complete.')

def csvReader(csvobj):
    with open(csvobj.path,'rU') as csvfile:
        next(csvfile)
        fields = csvobj.columns
        reader = csv.DictReader(csvfile,fieldnames=fields,quoting=csv.QUOTE_NONNUMERIC)
        for row in reader:
            yield row

