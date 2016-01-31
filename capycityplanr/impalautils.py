import logging, subprocess
from csvobject import CSVObject

#Invalidates metadata
def invalidateMetadata(csvobj):
    logging.info('Invalidating metadata')
    p = subprocess.Popen([csvobj.config.impala_path,'--query','invalidate metadata;','--print_header'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    logging.info(out)
    return 0

#Shows tables
def showTables(csvobj):
    logging.info('Getting Impala Tables...')
    p = subprocess.Popen([csvobj.config.impala_path,'--query','show tables;','--print_header'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    logging.info('\n' + out)
    return 0

#Runs query
def runQuery(query,csvobj):
    logging.info('Executing Impala Query: ' + query)
    p = subprocess.Popen([csvobj.config.impala_path,'--query',query,'--print_header'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    logging.info('\n' + out)
    return 0

def runQueryAndOutput(query,csvobj):
    logging.info('Executing Impala Query and saving as CSV')
    logging.info('Query: ' + query)
    output_location = csvobj.config.output_dir + '/' + csvobj.kite_class + '.csv'
    p = subprocess.Popen([csvobj.config.impala_path,'-B','--query',query,'-o',output_location,'--print_header','--output_delimiter=,'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    logging.info('Exporting results to ' + output_location)
    out,err = p.communicate()
    logging.info(out)
    return 0

def executeQueryPlan(csvobj):
    type = csvobj.type
    if type.lower() == 'hdfs':
        executeHdfsPlan(csvobj)
    elif type.lower() == 'yarn':
        executeYarnPlan(csvobj)
    elif type.lower() == 'impala':
        executeImpalaPlan(csvobj)
    else:
        logging.error('No defined query plans for type: ' + str(type))

def executeHdfsPlan(csvobj):
    table = csvobj.kite_class
    temp_table = table + '_temp'
    user_accounts = csvobj.getUserAccountsAsQuery()
    service_accounts = csvobj.getServiceAccountsAsQuery()
    query = 'create table ' + temp_table + ' as select cast((`date`/1000) as timestamp) as `date`, ((' + service_accounts + ')/(1024*1024*1024)) as services, ((' + user_accounts + ')/(1024*1024*1024)) as users from ' + table + ' where `date` is NOT NULL order by `date` ASC;'
    runQuery(query,csvobj)
    next_query = 'select `date`, services, users from ' + temp_table + ' where hour(`date`) = 12 AND services !=0 order by `date` ASC;'
    runQueryAndOutput(next_query,csvobj)
   
    
def executeYarnPlan(csvobj):
    table = csvobj.kite_class
    query = 'select cast(`timestamp` as timestamp) as `date`, SUM(value) as applications from ' + table + ' where metricname = "integral(apps_ingested_rate)" group by `timestamp` order by `timestamp`'
    runQueryAndOutput(query,csvobj)     

def executeImpalaPlan(csvobj):
    table = csvobj.kite_class
    query = 'select cast(`timestamp` as timestamp) as `date`, SUM(value) as queries from ' + table + ' where metricname = "integral(queries_ingested_rate)" group by `timestamp` order by `timestamp`'
    runQueryAndOutput(query,csvobj)

