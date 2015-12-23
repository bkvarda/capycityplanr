from csvobject import CSVObject
from configobject import ConfigObject
conf = ConfigObject('capycityplanr.conf')
obj = CSVObject('test/Capacity__HDFSDiskUsageByUserYTD.csv',conf)

print(obj.location())
print(conf.watchDir())
print(conf.kite_path)
print(obj.config.kite_path)
