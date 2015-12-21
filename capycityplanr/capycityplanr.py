import logging, cmcsv, ConfigParser



def main():
    #Config and logging
    Config = ConfigParser.ConfigParser()
    Config.read('capycityplanr.conf')
    logging_dir=Config.get('Directories','LoggingDirectory')
    watch_dir=Config.get('Directories','WatchDirectory')
    temp_dir=Config.get('Directories','TempDirectory')
    output_dir=Config.get('Directories','OutputDirectory')
    logfmt ='%(asctime)s - %(levelname)s - %(message)s'
    datefmt = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(filename=logging_dir+'/capycity.log',level=logging.DEBUG,datefmt=datefmt,format=logfmt)
    
    #Starting the actual process
    logging.info('Capycityplanr started')
    logging.info('Config file discovered with the following sections: ' + str(Config.sections()))
    cmcsv.scan_folder(watch_dir)
    csvs_to_process = cmcsv.extract_csvs('Capacity.zip',temp_dir)
    for csv in csvs_to_process:
        cmcsv.clean_csv(csv,output_dir) 
    logging.info('Capycityplanr ended')
main()
