import pandas as pd
from SoilDatabase import SoilDatabase as soilDB

class SoilMapAttributeTable:
    def __init__(self, path, nameCol='Nombre', format='none', sheetName = 'Sheet1'):
        self.path = path
        if path[-3:-1] == 'csv':
            self.table = pd.read_csv(path)
            print("Imported CSV file")
        else:
            self.table = pd.read_excel(path, sheet_name=sheetName)
            print("Imported Excel file")
        print("Processing...please wait...")
        self.format = format
        self.parameters = self.table.columns
        self.names = []
        for name in self.table[nameCol]:
            if name not in self.names:
                self.names.append(name)
        #for parameter in self.parameters:
        #    for i in range(len(self.table[parameter])):
        #        if self.table[parameter][i].isnull:
        #            self.table[parameter][i] = "-"
        self.defined = self.areDefined(self.parameters)
        self.definedParms = []
        for i in range(len(self.parameters)):
            if self.defined[i]:
                self.definedParms.append(self.parameters[i])

    def print(self):
        print(self.table)

    def printDefined(self):
        print(self.table[self.definedParms])

    def getDefined(self):
        return self.table[self.definedParms]

    def areDefined(self, parameters):
        areDefined = []
        for parameter in parameters:
            areDefined.append(self.isDefined(parameter))
        return areDefined

    def isDefined(self, parameter):
        return not(
            all(self.table[parameter][:] == 0) or all(self.table[parameter][:] == '-') or all(self.table[parameter] == ' '))

    def createSoilDB(self):
        return soilDB(self)