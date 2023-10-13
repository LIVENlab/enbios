import bw2data as bd

import bw2io as bi
#Create a new project. To do so, you need the bw2data
bd.projects.set_current('Seeds_exp4')

"""
We're going to import ecoinvent in our new project
"""
#Spold files here

spold_files = r'C:\Users\altz7\UAB\LIVENlab - SEEDS - _Alex\ecoinvent\datasets'            #Import spolds from local

# Load biosphere3 and LCIA Methods
bi.bw2setup()

# Loads data from ecoinvent in our RAM. Still not written in our hard drive.
ei = bi.SingleOutputEcospold2Importer(spold_files, "db_experiments", use_mp=False)
# Link the processes among themselves and to the biosophere
ei.apply_strategies()
ei.statistics()
# ei.statistics() (per saber si tinc tots els exchanges linked)

# Save the database in the hard drive
ei.write_database()
# bd.databases (per comprovar si la database s'ha instal·lat)

print(bd.databases)  # check if database is there


bd.projects.set_current('Seeds_exp4')            # Select your project
myei = bd.Database('db_experiments')
print(len(myei))
