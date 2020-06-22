import sqlite3 as sq
import os.path

class FormatDatabase:
    def __init__(self, format, RequiredParameters, AdditionalParameters):
        self.format = format
        self.previous = os.path.isfile(self.format + ".db")
        self.RequiredParameters = RequiredParameters
        self.AdditionalParameters = AdditionalParameters
        if not self.previous:
                self.create()
                self.fill()


    def create(self):
        conn = sq.connect(self.format + ".db")
        c = conn.cursor()
        c.execute("""CREATE TABLE format (
                        Variable text,
                        UserName text)""")
        conn.commit()
        print("Please enter the names of the columns in your data corresponding to the printed nariable names")
        print("To skip a variable enter: 'F'    Note: please make sure that you have the correct units")
        print("For layer specific variable names please use a '*' to note the location of the integer showing the layer number")
        self.translated = [()]
        entryRecord = []
        for parameter in self.RequiredParameters:
            entryRecord.append(input(parameter[0] + " " + parameter[1] + ": "))

        if entryRecord.__contains__("F"):
            for parameter in self.AdditionalParameters:
                entryRecord.append(input(parameter[0] + " " + parameter[1] + ": "))
        for i in range(len(entryRecord)):
            if i < len(self.RequiredParameters) and type(entryRecord[i]) == "str":
                c.execute("""INSERT INTO format VALUES (?,?)""", (self.RequiredParameters[i][0], entryRecord[i]))
            elif type(entryRecord[i] == "str"):
                c.execute("""INSERT INTO format VALUES (?,?)""", (
                    self.AdditionalParameters[i-len(self.RequiredParameters)][0], entryRecord[i]))
            else:
                print("The values entered weren't of the correct type: STRING")
        conn.commit()
        c.execute("SELECT * FROM format")
        contents = c.fetchall()

        for content in contents:
            print(content)
        conn.close()

    def translateOne(self, parameter):
        conn = sq.connect(self.format + ".db")
        c = conn.cursor()
        c.execute("SELECT * FROM format WHERE Variable = (?)", (parameter,))
        result = c.fetchone()
        conn.close()
        return result[0]


    def translateMany(self, parameters):
        conn = sq.connect(self.format + ".db")
        c = conn.cursor()
        results = []
        for parameter in parameters:
            c.execute("SELECT * FROM format WHERE Variable = (?)", (parameter,))
            results.append(c.fetchall())
        conn.close()
        return results[:][0]