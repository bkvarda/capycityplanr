import logging, cmcsv, pykite, time, avroutils, hdfsutils, impalautils, sys
from config import Config
from csvobject import CSVObject

def main():
    #Config and logging
    config = Config('capycityplanr.conf')
    logfmt ='%(asctime)s - %(levelname)s - %(message)s'
    datefmt = '%m/%d/%Y %I:%M:%S'
    logging.basicConfig(filename=config.logging_dir+'/capycity.log',level=logging.DEBUG,datefmt=datefmt,format=logfmt)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(logfmt))
    logging.getLogger('').addHandler(console)
    accounts = [] 
    #Starting the actual process
    logging.info('Capycityplanr started')
    cmcsv.scan_folder(config.watch_dir)
    csvs_to_process = cmcsv.extract_csvs(str(sys.argv[1]),config)
    for csvobj in csvs_to_process:
        csvobj = cmcsv.clean_csv(csvobj)
        if csvobj.type == 'hdfs':
            service_accounts = ''
            user_accounts = ''
            for account in csvobj.getServiceAccounts():
                service_accounts += account + '+'
            for account in csvobj.getUserAccounts():
                user_accounts += account + '+'
            print('Service accounts are: ' + service_accounts)
            print('User accounts are: ' + user_accounts)
        pykite.infer_schema(csvobj)
        pykite.create_dataset(csvobj)
        attempts = 1
        #Try data insertion several times due to ulimit issues. There's probably a better way.
        while attempts < 4:
            code = pykite.import_data(csvobj)
            logging.info('Attempt to import data #' + str(attempts))
            if code == 1:
                logging.warning('Attempt # ' + str(attempts) + ' failed. Attempting again')
                attempts +=1
            else:
                attempts +=4
        impalautils.invalidateMetadata(csvobj)
        impalautils.showTables(csvobj)
        query = "select * from " + csvobj.kite_class
        impalautils.runQueryAndOutput(query,csvobj) 
    logging.info('Capycityplanr ended')
main()
