import logging, subprocess


#Infers the schema. Data should be dictionary with type, customer, and csv path
def infer_schema(kite_path,data):
    csv_path = data['path']
    type = data['type']
    customer = data['customer']
    kite_class = customer + '_' + type
    logging.info('Inferring schema for ' + csv_path)
    p = subprocess.Popen([kite_path,'csv-schema',csv_path,'--class',kite_class,'-o',kite_class+'.avsc'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
    else:
        logging.info(out)
    logging.info('Schema inference complete')
    data['schema'] = kite_class + '.avsc'
    data['class'] = kite_class
    return data

#Creates Kite dataset
def create_dataset(kite_path,data):
    kite_class = data['class']
    schema = data['schema']
    csv_path = data['path']
    logging.info('Creating dataset with title ' + kite_class + ' from CSV ' + csv_path)
    p = subprocess.Popen([kite_path,'-v','create',kite_class,'-s',schema],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
    else:
        logging.info(out)
    logging.info('Dataset created')
    return data

#Inserts CSV data into Hive (and Impala)
def import_data(kite_path,data):
    kite_class = data['class']
    csv_path = data['path']
    logging.info('Inserting data into Hive table ' + kite_class)
    p = subprocess.Popen([kite_path,'-v','csv-import',csv_path,kite_class],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
    else:
        logging.info(out)
        
