from SoilMapAttributeTable import SoilMapAttributeTable as SMAT
import os

#os.remove("Soils.db")
os.remove("UCSSolola.db")
ucsSolola = SMAT(path="C:\\Users\\Joel\Documents\\Prof Adamowski\\Iximulew\\SWAT+\\csv\\UCS Solola.xlsx", nameCol="PERF_MODAL",
                 format="UCSSolola")
ucsSolola.printDefined()
ucsSolola.createSoilDB()
print("Program Finished")
