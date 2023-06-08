import pandas as pd

loeMasterData = pd.read_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\KOC Datawarehouse\loe\loeMaster.xlsx")

print(len(loeMasterData))

loeSortedMasterData = loeMasterData.sort_values(
    by=["EffDate", "SubAccount"])

print("done")
