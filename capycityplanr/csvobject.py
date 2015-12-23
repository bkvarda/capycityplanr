#Class for handling CSV data
class CSVObject():

    def __init__(self, path, config, type=None, columns=None, customer=None):
        self.path = path
        self.type = type
        self.columns = columns
        self.customer= customer
        self.config = config

    def setPath(self,input):
        self.path = input
    def setType(self,input):
        self.type = input
    def setColumns(self,input):
        self.columns = input
    def setCustomer(self,input):
        self.customer = input
    #takes list of columns (accounts) and distingueshes hadoop services from users
    def getServiceAccounts(self):
        column_list = self.columns
        cdh_service_list = self.config.cdh_service_accounts
        service_accounts = []
        user_accounts = []
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
        return service_accounts
     #Returns list of columns (accounts) and distingueshes hadoop services from users
    def getUserAccounts(self):
        column_list = self.columns
        cdh_service_list = self.config.cdh_service_accounts
        service_accounts = []
        user_accounts = []
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
        return user_accounts
