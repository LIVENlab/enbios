import bw2data as bd
from bw2data.backends import Activity

bd.projects.set_current("ecoi_dbs")

method = ('CML v4.8 2016 no LT',
          'climate change no LT',
          'global warming potential (GWP100) no LT')

# get main db
db = bd.Database("cutoff391")

# activity: Activity = db.random()
activity = db.get('8cb1567c3ff743d191b309dd64ad84f2')
# 'chromium steel drilling, computer numerical controlled' (kilogram, RoW, None) '8cb1567c3ff743d191b309dd64ad84f2'

# copy some activity to a new db
act_copy = activity.copy("TEST_COPY")
# new_db = bd.Database("new_db")
# new_db.register()
# act_copy["database"] = "new_db"

new_db = bd.Database("new_db")
act_copy = list(new_db)[0]
act_copy_lcia = act_copy.lca(method).score

print(act_copy_lcia)

list(act_copy.exchanges())

for link in activity.exchanges():
    print("IN", link.input, "OUT", link.output, "AMOUNT", link.amount)

for link in activity.upstream():
    print("IN", link.input, "OUT", link.output, "AMOUNT", link.amount)
# ex = myact.new_exchange(input=wind, type="technosphere", amount=1)
# ex.save()




act_copy.save()


#