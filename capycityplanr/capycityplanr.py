import logging, cmcsv, ConfigParser, pykite



def main():
    #Config and logging
    Config = ConfigParser.ConfigParser()
    Config.read('capycityplanr.conf')
    logging_dir=Config.get('Directories','LoggingDirectory')
    watch_dir=Config.get('Directories','WatchDirectory')
    temp_dir=Config.get('Directories','TempDirectory')
    output_dir=Config.get('Directories','OutputDirectory')
    cdh_service_accounts=Config.get('Services','ServiceAccounts').split(",")
    kite_path=Config.get('Kite','KitePath')
    logfmt ='%(asctime)s - %(levelname)s - %(message)s'
    datefmt = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(filename=logging_dir+'/capycity.log',level=logging.DEBUG,datefmt=datefmt,format=logfmt)
    accounts = [] 
    #Starting the actual process
    logging.info('Capycityplanr started')
    logging.info('Config file discovered with the following sections: ' + str(Config.sections()))
    logging.info('Configured CDH service accounts are: ' + str(cdh_service_accounts))
    logging.info('Testing array ' + cdh_service_accounts[0])
    cmcsv.scan_folder(watch_dir)
    csvs_to_process = cmcsv.extract_csvs('Capacity.zip',temp_dir)
    for csv in csvs_to_process:
        output = cmcsv.clean_csv(csv,output_dir)
        if output['type'] == 'hdfs':
            accounts = cmcsv.get_service_accounts(output['columns'],cdh_service_accounts)
        data = pykite.infer_schema(kite_path,output)
        data = pykite.create_dataset(kite_path,data)
        pykite.import_data(kite_path,data) 
    logging.info('Capycityplanr ended')
main()
