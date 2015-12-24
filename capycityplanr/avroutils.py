import logging, avro.schema, csv, csvobject
from csvobject import CSVObject
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

#Converts csv to avro
def csvToAvro(csvobj):
    schema = avro.schema.parse(open(csvobj.schema).read())
    avro_path = csvobj.config.temp_dir + '/' + csvobj.customer + '_' + csvobj.type + '.avro'
    logging.info('Converting CSV to Avro')
    logging.info('CSV Path: ' + csvobj.path + ', Avro Output Path: ' + avro_path)
    snappy_writer = DataFileWriter(open(avro_path, 'wb'), DatumWriter(), schema, codec="null")
    
    fields = csvobj.columns
    headers = dict([(v,i) for i,v in enumerate(fields)])
    with open(csvobj.path,'rU') as csvfile:
        reader = csv.reader(csvfile)
        reader.next() # skip header
        for boring_row in reader:
            row = dict(zip(fields, boring_row))

            snappy_writer.append(row)

    snappy_writer.close()
    csvobj.setAvro(avro_path)
    logging.info('Conversion of CSV to Avro complete.')
