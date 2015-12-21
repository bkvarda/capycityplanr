import logging, cmcsv, ConfigParser



def main():
    Config = ConfigParser.ConfigParser()
    Config.read('capycityplanr.conf')
    logging_dir=Config.get('Directories','LoggingDirectory')
    watch_dir=Config.get('Directories','WatchDirectory')
    temp_dir=Config.get('Directories','TempDirectory')
    output_dir=Config.get('Directories','OutputDirectory')
    logfmt ='%(asctime)s - %(levelname)s - %(message)s'
    datefmt = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(filename=logging_dir+'/capycity.log',level=logging.DEBUG,datefmt=datefmt,format=logfmt)
    logging.info('Capycityplanr started')
    logging.info('Config file discovered with the following sections: ' + str(Config.sections()))
    cmcsv.scan_folder(watch_dir)
    cmcsv.extract_csvs('Capacity.zip',output_dir)
    logging.info('Capycityplanr ended')
main()
