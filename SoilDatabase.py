import sqlite3 as sq
import os.path
from FormatDatabase import FormatDatabase

class SoilDatabase:
    def __init__(self, SMAT):
        self.SMAT = SMAT
        self.table = self.SMAT.getDefined().drop_duplicates()
        format = SMAT.format
        self.RequiredParameters = [("NumberOfLayers", "INTEGER"), ("SoilHydrologicGroup", "A,B,C,D"),
                              ("MaximumRootingDepth", "mm"), ("AnionExclusion", "default=0.5"),
                              ("MaxFractionCrackVolume", "*/*"), ("LayerDepth", "mm"),
                              ("LayerMoistBulkDensity", "*/*"), ("LayerPlantAvailableWater","mm H2O/mm"),
                              ("LayerSaturatedHydraulicConductivity", "mm/hr"), ("LayerOrganicCarbon", "%weight"),
                              ("LayerClay", "%weight"),	("LayerSilt", "%weight"), ("LayerSand", "%weight"),
                              ("LayerRockContent", "%weight"), ("LayerMoistAlbedo", ""),
                              ("LayerUSLEsoilErodability", ""), ("LayerElectricalConductivity", "dS/m"),
                              ("Layer[CaCO3]", "% (0%-50%)"), ("LayerSoilPH", "(3-10)")]

        self.AdditionalParameters = [("LayerPlantAvailableWaterVol", "%volume"), ("LayerFieldCapacityMm", "mmH2O/mm"),
                                ("LayerFieldCapacityVol", "%vol"), ("LayerWaterContent30kPaMm", "mmH2O/mm"),
                                ("LayerWaterContent30kPaVol", "%volume"), ("LayerRealDenistyOfSoil", "*/*"),
                                ("LayerDepthsCm", "cm")]

        if os.path.isfile(format + ".db"):
            self.formatDB = FormatDatabase(format, self.RequiredParameters, self.AdditionalParameters)
        else:
            self.formatDB = FormatDatabase(format, self.RequiredParameters, self.AdditionalParameters)
        self.definedParmsUser = SMAT.definedParms
        if not os.path.isfile("Soils.db"):
            conn = sq.connect("Soils.db")
            c = conn.cursor()
            c.execute("""CREATE TABLE soils (
                                SNAM        text,
                                NLAYERS     integer,
                                HYDGRP      text,
                                SOL_ZMX     integer,
                                ANION_EXCL  real,
                                SOL_CRK     real,
                                SOL_ZL      blob,
                                SOL_BDL     blob,
                                SOL_AWCL    blob,
                                SOL_KL      blob,
                                SOL_CBNL    blob,
                                CLAYL       blob,
                                SILTL       blob,
                                SANDL       blob,
                                ROCKL       blob,
                                SOL_ALBL    blob,
                                USLE_KL     blob,
                                SOL_ECL     blob,
                                SOL_CALL    blob,
                                SOL_PHL     blob
                                )""")
            conn.commit()
        else:
            conn = sq.connect("Soils.db")
            c = conn.cursor()
        c.execute("SELECT * FROM soils")

        if len(c.fetchall()) == 0:
            conn.close()
            self.fill()
        elif \
                input(
                    "Please enter 'F' if you do not want to automatically update the database (anything else to do it): ")\
                        == 'F':
            conn.close()
            self.update()


    def fill(self):
        conn = sq.connect("Soils.db")
        c = conn.cursor()
        self.TranslatedParameter = {variable[0]: self.formatDB.translateOne(variable[0]) for variable in self.RequiredParameters}
        if all(self.TranslatedParameter[self.RequiredParameters[:][0]] != "F"):
            Nlayers = self.table.loc[self.TranslatedParameter["NumberOfLayers"]]
            Hydgrp = self.table.loc[self.TranslatedParameter["SoilHydrologicGroup"]]
            SolZmx = self.table.loc[self.TranslatedParameter["MaximumRootingDepth"]]
            AnionExclusion = self.table.loc[self.TranslatedParameter["AnionExclusion"]]
            SolCrk = self.table.loc[self.TranslatedParameter["MaxFractionCrackVolume"]]
            for i in range(max(Nlayers)):
                SolZL, SolBdL, SolAwcL, SolKL, SolCbnL, ClayL, SiltL, SandL, RockL, SolAlbL, UsleKL, SolEcL, SolCalL,\
                SolPhL = []
                SolZL.append(tuple(self.table.loc[self.TranslatedParameter["LayerDepth"].split("*")[0] + str(i)]))
                SolBdL.append(tuple(self.table.loc[self.TranslatedParameter["LayerMoistBulkDensity"].split("*")[0]
                                                   + str(i)]))
                SolAwcL.append(tuple(self.table.loc[self.TranslatedParameter["LayerPlantAvailableWater"].split("*")[0]
                                                    + str(i)]))
                SolKL.append(
                    tuple(self.table.loc[self.TranslatedParameter["LayerSaturatedHydraulicConductivity"].split("*")[0]
                                         + str(i)]))
                SolCbnL.append(tuple(self.table.loc[self.TranslatedParameter["LayerOrganicCarbon"].split("*")[0] + str(i)]))
                ClayL.append(tuple(self.table.loc[self.TranslatedParameter["LayerClay"].split("*")[0] + str(i)]))
                SiltL.append(tuple(self.table.loc[self.TranslatedParameter["LayerSilt"].split("*")[0] + str(i)]))
                SandL.append(tuple(self.table.loc[self.TranslatedParameter["LayerSand"].split("*")[0] + str(i)]))
                RockL.append(tuple(self.table.loc[self.TranslatedParameter["LayerRockContent"].split("*")[0] + str(i)]))
                SolAlbL.append(tuple(self.table.loc[self.TranslatedParameter["LayerMoistAlbedo"].split("*")[0] +
                                                    str(i)]))
                UsleKL.append(tuple(self.table.loc[self.TranslatedParameter["LayerUSLEsoilErodability"].split("*")[0]+
                                                   str(i)]))
                SolEcL.append(tuple(self.table.loc[self.TranslatedParameter["LayerElectricalConductivity"].split("*")[0]
                                                   + str(i)]))
                SolCalL.append(tuple(self.table.loc[self.TranslatedParameter["Layer[CaCO3]"].split("*")[0] + str(i)]))
                SolPhL.append(tuple(self.table.loc[self.TranslatedParameter["LayerSoilPH"].split("*")[0] + str(i)]))

        else:
            self.TranslatedParameter.update({variable[0]: self.formatDB.translateOne(variable[0])
                                             for variable in range(len(self.AdditionalParameters))})
            print("Missing parameters will be calculated from Additional Parameters")

        c.executemany("""INSERT INTO soils VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (str(self.SMAT.names), Nlayers, Hydgrp, SolZmx, AnionExclusion, SolCrk, SolZL,
                           SolBdL, SolAwcL, SolKL, SolCbnL, ClayL, SiltL, SandL, RockL, SolAlbL, UsleKL, SolEcL,
                           SolCalL, SolPhL))
        conn.commit()
        conn.close()