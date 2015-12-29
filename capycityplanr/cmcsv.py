import logging, os, csv, zipfile, contextlib, shutil, tempfile, subprocess
from csvobject import CSVObject


def scan_folder(folder):
    #Scans folder, returns any new zips to be unzipped
    logging.info('Scanning folder')
    
#returns list of CSVObjects
def extract_csvs(file,config):
    #Extracts csv contents and returns dictionary with csv type as key and location as value
    logging.info('Extracing zip: ' + file)
    #We assume that the customer (and table prefix) name is the name of the zip. 
    customer_name = os.path.splitext(file)[0]
    output_folder = config.temp_dir
    logging.info('Zip contents are for customer: ' + customer_name)
    csvs_to_process = []
    with contextlib.closing(zipfile.ZipFile(file, "r")) as zip:
        logging.info('Extracting contents of ' + file + ' to ' + output_folder)
        zip_files = zip.namelist()
        for member in zip_files:
            if ".csv" in member:
                logging.info('CSV identified in ' + file + '- named: ' + member + ' - sending to ' + output_folder)
                filename = customer_name + '_' + os.path.basename(member)
                source = zip.open(member)
                save_location = os.path.join(output_folder, filename)
                target = open(save_location, "wb")
                logging.info('Extracting ' + filename + ' to ' + save_location)
                shutil.copyfileobj(source, target)
                #add to our list of CSVs that now need to be processed/cleaned
                csvobj = CSVObject(path=save_location,type=get_csv_type(filename),customer=customer_name,config=config)
                csvs_to_process.append(csvobj)
    zip.close()
    logging.info('Extraction of ' + file + ' completed')
    logging.info('List of files to process :' + str(csvs_to_process))
    return csvs_to_process

def clean_csv(csvobj):
    #Takes dictionary of csv type and location and cleans up so that columns are unique and fit Hive schema
    columns = {}
    unique_columns = []
    payload = {}
    logging.info('Cleaning CSV ' + os.path.basename(csvobj.path))
    logging.info('CSV is type ' + csvobj.type + ' for customer ' + csvobj.customer)
    #Here we get rid of special characters such as ^M that the csv library can't handle
    file_backup = csvobj.path + '.bak'
    with tempfile.NamedTemporaryFile(delete=False) as fh:
        for line in open(csvobj.path):
            line = line.rstrip()
            line = line.replace('\r', '')
            fh.write(line + '\n')
    os.rename(csvobj.path, file_backup)
    os.rename(fh.name, csvobj.path)    
    os.remove(file_backup) 
    #Here we parse the columns and get rid of not-unique columns and formatting that doesn't work in Hive
    counter = 0
    os.rename(csvobj.path, file_backup)
    with open (file_backup, 'rU') as input:
        with open (csvobj.path, 'wb') as output:
            writer = csv.writer(output)
            for row in csv.reader(input):
                #hdfs type has csv header, this effectively removes it
                if counter == 0 and csvobj.type == 'hdfs':
                    pass 
                #these are columns that need to be parsed
                elif (counter == 0 and csvobj.type != 'hdfs') or (counter == 1 and csvobj.type == 'hdfs'):
                    index = 0
                    for column in row:
                        #if column not unique
                        if str(columns.get(column.lower())) != 'None':
                            #make it unique
                            logging.info(column + ' is not unique. Appending index')
                            new_column_name = column + str(index)
                            columns[new_column_name] = index
                        #else column is unique
                        else:
                            columns[column] = index
                        
                        index += 1
                    #Now we have a dict with unique columns, we need to order them correctly and remove special characters
                    for column in columns:
                        clean_column = column.translate(None,'\\*./!?#- ')
                        number = columns[column]
                        unique_columns.insert(number, str(clean_column))
                    logging.info('Inserting the following unique columns into the CSV: ' + str(unique_columns))
                    writer.writerow(unique_columns)        
                #this are all data rows that can just be inserted
                else:
                    writer.writerow(row)
                
                counter += 1
    os.remove(file_backup)
    csvobj.setColumns(unique_columns)
    return csvobj

def get_csv_type(file):
    if "hdfs" in file.lower():
        return "hdfs"
    elif "impala" in file.lower():
        return "impala"
    elif "yarn" in file.lower():
        return "yarn"
    else:
        return "unknown"

