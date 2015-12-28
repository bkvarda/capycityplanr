import logging, subprocess

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
    output_location = csvobj.config.output_dir + '/' + csvobj.kite_class + '.csv'
    p = subprocess.Popen([csvobj.config.impala_path,'-B','--query',query,'-o',output_location,'--print_header','--output_delimiter=,'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    logging.info('Exporting results to ' + output_location)
    out,err = p.communicate()
    logging.info(out)
    return 0
