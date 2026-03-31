import pandas as pd

df_mar = pd.read_excel("20260301-20260327_sales_details.xlsx", dtype={"SKU no": str})

df_mar = df_mar[["SKU no","SKU title","Volume","Amount"]]
df_mar['SKU no'] = df_mar['SKU no'].str.zfill(5)
df_mar["SKU no"] = df_mar["SKU no"].astype(str).replace(" ","")
df_mar_grouped = df_mar.groupby("SKU no").agg({
    "Volume": "sum",
    "Amount": "sum"
}).reset_index()

df_mar_grouped = df_mar_grouped.rename(columns={"SKU no": "SKU No.","Volume":"三月銷售量","Amount":"三月銷售額"})
df_mar_grouped["SKU No."] = df_mar_grouped["SKU No."].astype(str).str.strip()

df_mar.rename(columns={"SKU no": "SKU No.","SKU title":"品名"},inplace=True)
df_mar["SKU No."] = df_mar["SKU No."].astype(str).str.strip()
orders_unique = df_mar.drop_duplicates(subset=['SKU No.'], keep='first')
df_mar_res = df_mar_grouped.merge(orders_unique, on='SKU No.', how='left')
df_mar_res = df_mar_res[["SKU No.","品名","三月銷售量","三月銷售額"]]

print(df_mar_res)