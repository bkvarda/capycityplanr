#Object to hold configuration
import ConfigParser, os
class Config:
    def __init__(self,config_file):
        Config = ConfigParser.ConfigParser()
        Config.read(config_file)
        self.logging_dir=Config.get('Directories','LoggingDirectory')
        self.watch_dir=Config.get('Directories','WatchDirectory')
        self.temp_dir=Config.get('Directories','TempDirectory')
        self.output_dir=Config.get('Directories','OutputDirectory')
        self.cdh_service_accounts=Config.get('Services','ServiceAccounts').split(",")
        self.kite_path=Config.get('Kite','KitePath')
        self.hdfs_output_dir=Config.get('HDFS','OutputDirectory')
        self.impala_path=Config.get('Impala','ImpalaPath')
        self.checkDirectories()
    def checkDirectories(self):
        if not os.path.isdir(self.watch_dir):
            os.makedirs(self.watch_dir)
        if not os.path.isdir(self.temp_dir):
            os.makedirs(self.temp_dir)
        if not os.path.isdir(self.logging_dir):
            os.makedirs(self.logging_dir)
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)

        
