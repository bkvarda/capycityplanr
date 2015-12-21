import logging, os, csv, zipfile, contextlib, shutil


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

def clean_csv(csv,output_folder):
    #Takes dictionary of csv type and location and cleans up so that columns are unique and fit Hive schema
    customer_name = csv['customer']
    type = csv['type']
    file = csv['path']
    logging.info('Cleaning CSV ' + os.path.basename(file))
    logging.info('CSV is type ' + type + ' for customer ' + customer_name)
    #Here we get rid of special characters such as ^M that the csv library can't handle
    file_backup = file + '.bak'
    os.rename(file, file_backup)
    with open(file_backup, 'rU') as infile:
        with open(file, 'w') as outfile:
            for line in infile:
                outfile.write(line.replace('\r', ''))
    os.remove(file_backup)
    
    #Here we parse the columns and get rid of not-unique columns and formatting that doesn't work in Hive

def get_csv_type(csv):
    if "hdfs" in csv.lower():
        return "hdfs"
    elif "impala" in csv.lower():
        return "impala"
    elif "yarn" in csv.lower():
        return "yarn"
    else:
        return "unknown"


