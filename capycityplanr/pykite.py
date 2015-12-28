import logging, subprocess


#Infers the schema. Data should be dictionary with type, customer, and csv path
def infer_schema(csvobj):
    csv_path = csvobj.path
    type = csvobj.type
    customer = csvobj.customer
    kite_class = customer + '_' + type
    save_location = csvobj.config.temp_dir + '/' + kite_class + '.avsc'
    logging.info('Inferring schema for ' + csv_path)
    p = subprocess.Popen([csvobj.config.kite_path,'csv-schema',csv_path,'--class',kite_class,'-o',save_location],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
        return 1
    else:
        logging.info(out)
        logging.info('Schema inference complete')
        csvobj.setSchema(save_location)
        csvobj.setKiteClass(kite_class)
        return 0

#Creates Kite dataset
def create_dataset(csvobj):
    kite_class = csvobj.kite_class
    schema = csvobj.schema
    csv_path = csvobj.path
    logging.info('Creating dataset with title ' + kite_class + ' from CSV ' + csv_path)
    p = subprocess.Popen([csvobj.config.kite_path,'-v','create',kite_class,'-s',schema],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
        return 1
    else:
        logging.info(out)
        logging.info('Dataset created')
        return 0

#Inserts CSV data into Hive (and Impala)
def import_data(csvobj):
    kite_class = csvobj.kite_class
    csv_path = csvobj.path
    logging.info('Inserting data into Hive table ' + kite_class)
    p = subprocess.Popen([csvobj.config.kite_path,'-v','csv-import',csv_path,kite_class],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
        return 1
    else:
        logging.info(out)
        logging.info('CSV data inserted into Hive/Impala')
        return 0
