import logging, subprocess, os

#copies avro file and schema to HDFS
def copyToHdfs(csvobj):
    avro_path = csvobj.avro
    schema_path = csvobj.schema
    hdfs_location = csvobj.config.hdfs_output_dir
    logging.info('Copying Avro ' + avro_path + ' to HDFS path: ' + hdfs_location)
    p = subprocess.Popen(['hdfs','dfs','-put',avro_path,hdfs_location],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
    else:
        logging.info(out)
        logging.info('Copy of Avro to HDFS completed')
    csvobj.setAvro('hdfs://' + hdfs_location + '/' + os.path.basename(avro_path))
    

    logging.info('Copying Avro schema ' + schema_path + ' to HDFS path: ' + hdfs_location)
    p = subprocess.Popen(['hdfs','dfs','-put',schema_path,hdfs_location],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err = p.communicate()
    if err:
        logging.error(err)
    else:
        logging.info(out)
        logging.info('Copy of Avro schema to HDFS completed')
    csvobj.setSchema('hdfs://' + hdfs_location + '/' + os.path.basename(schema_path))
