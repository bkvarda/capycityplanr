import logging, os, csv, zipfile, contextlib, shutil, tempfile


def scan_folder(folder):
    #Scans folder, returns any new zips to be unzipped
    logging.info('Scanning folder')
    

def extract_csvs(file,output_folder):
    #Extracts csv contents and returns dictionary with csv type as key and location as value
    logging.info('Extracing zip: ' + file)
    #We assume that the customer (and table prefix) name is the name of the zip. 
    customer_name = os.path.splitext(file)[0]
    logging.info('Zip contents are for customer: ' + customer_name)
    csvs_to_process = []
    with contextlib.closing(zipfile.ZipFile(file, "r")) as zip:
        logging.info('Extracting contents of ' + file + ' to ' + output_folder)
        zip_files = zip.namelist()
        for member in zip_files:
            if ".csv" in member:
                logging.info('CSV identified in ' + file + '- named: ' + member + ' - sending to ' + output_folder)
                filename = customer_name + '__' + os.path.basename(member)
                source = zip.open(member)
                save_location = os.path.join(output_folder, filename)
                target = open(save_location, "wb")
                logging.info('Extracting ' + filename + ' to ' + save_location)
                shutil.copyfileobj(source, target)
                #add to our list of CSVs that now need to be processed/cleaned
                csv_info = {}
                csv_info["path"] = save_location
                csv_info["type"] = get_csv_type(filename)
                csv_info["customer"] = customer_name
                csvs_to_process.append(csv_info)
    zip.close()
    logging.info('Extraction of ' + file + ' completed')
    logging.info('List of files to process :' + str(csvs_to_process))
    return csvs_to_process

def clean_csv(data,output_folder):
    #Takes dictionary of csv type and location and cleans up so that columns are unique and fit Hive schema
    customer_name = data['customer']
    type = data['type']
    file = data['path']
    columns = {}
    unique_columns = []
    payload = {}
    logging.info('Cleaning CSV ' + os.path.basename(file))
    logging.info('CSV is type ' + type + ' for customer ' + customer_name)
    #Here we get rid of special characters such as ^M that the csv library can't handle
    file_backup = file + '.bak'
    with tempfile.NamedTemporaryFile(delete=False) as fh:
        for line in open(file):
            line = line.rstrip()
            line = line.replace('\r', '')
            fh.write(line + '\n')
    os.rename(file, file_backup)
    os.rename(fh.name, file)    
    os.remove(file_backup) 
    #Here we parse the columns and get rid of not-unique columns and formatting that doesn't work in Hive
    counter = 0
    os.rename(file, file_backup)
    with open (file_backup, 'rU') as input:
        with open (file, 'wb') as output:
            writer = csv.writer(output)
            for row in csv.reader(input):
                #hdfs type has csv header, this effectively removes it
                if counter == 0 and type == 'hdfs':
                    pass 
                #these are columns that need to be parsed
                elif (counter == 0 and type != 'hdfs') or (counter == 1 and type == 'hdfs'):
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
    payload['path'] = file
    payload['type'] = type
    payload['columns'] = unique_columns
    return payload
def get_csv_type(file):
    if "hdfs" in file.lower():
        return "hdfs"
    elif "impala" in file.lower():
        return "impala"
    elif "yarn" in file.lower():
        return "yarn"
    else:
        return "unknown"

#takes list of columns (accounts) and distingueshes hadoop services from users
def get_service_accounts(column_list,cdh_service_list):
    service_accounts = []
    user_accounts = []
    accounts = {}
    for column in column_list:
        match = 0
        for service in cdh_service_list:
            
            if match > 0:
                break
            #if it is a service, add it to service_accounts
            elif column.translate(None,'\\*./!?#- ') == service.translate(None, '\\*./!?#- '):
                service_accounts.append(column)
                match += 1
        if match == 0:
            user_accounts.append(column)
    accounts['service_accounts'] = service_accounts
    accounts['user_accounts'] = user_accounts
    return accounts
