import logging, os, csv, zipfile, contextlib


def scan_folder(folder):
    #Scans folder, returns any new zips to be unzipped
    logging.info('Scanning folder')
    

def extract_csvs(file,output_folder):
    #Extracts csv contents and returns dictionary with csv type as key and location as value
    logging.info('Extracing zip: ' + file)
    with contextlib.closing(zipfile.ZipFile(file, "r")) as zip:
        logging.info('Extracting contents of ' + file + ' to ' + output_folder)
        #zip.printdir()
        print(zip.namelist())
        zip.extractall(output_folder)
    logging.info('Extraction of ' + file + ' completed')
  

def clean_csv(output_folder,csv):
    #Takes dictionary of csv type and location and cleans up so that columns are unique and fit Hive schema
    logging.info('Cleaning CSV')




