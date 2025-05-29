# Databricks notebook source
# DBTITLE 1,Creating Schemas
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS BRONZE;
# MAGIC CREATE SCHEMA IF NOT EXISTS SILVER;
# MAGIC CREATE SCHEMA IF NOT EXISTS GOLD;

# COMMAND ----------

# DBTITLE 1,Creating tables
for i in dbutils.fs.ls("./FileStore/tables"):
    table_name = i.name.split(".")[0]
    spark.sql(f"create table if not exists bronze.{table_name}")

# COMMAND ----------

# DBTITLE 1,Creating Bronze tables
from pyspark.sql.functions import split
for i in dbutils.fs.ls("./FileStore/tables"):
    file_format = i.name.split(".")[-1]
    table_name = i.name.split(".")[0]
    df = spark.read.format(file_format)\
        .option("cloudFiles.format", file_format)\
        .option("cloudFiles.schemaLocation", i.path)\
        .load(i.path)
    df.write.mode("append").option("mergeSchema","True").saveAsTable(f"bronze.{table_name}")


# COMMAND ----------

# DBTITLE 1,creating silver tables
from delta.tables import *
tbls = spark.sql("Show Tables in bronze")
tbls = tbls.where("isTemporary != true")
tbls_lst = [i[0] for i in tbls.select("tableName").collect()]
primary_keys = {"media_customer_reviews":"new_id",
                "media_gold_reviews_chunked":"chunk_id",
                "sales_customers":"customerID",
                "sales_franchises":"franchiseID",
                "sales_suppliers":"supplierID",
                "sales_transactions":"transactionID"}

for i in tbls_lst:
    pk = primary_keys[i]
    brnz_df = spark.read.table(f"bronze.{i}")
    brnz_df.createOrReplaceTempView("brnz_tbl")
    tbl_exist = spark.catalog.tableExists(f"silver.{i}")
    if tbl_exist:
        sil_df = spark.read.table(f"silver.{i}")
        sil_df.createOrReplaceTempView("sil_tbl")
        spark.sql(f"""merge into sil_tbl using brnz_tbl on
              sil_tbl.{pk} == brnz_tbl.{pk}
              when matched then update set *
              when not matched then insert *""" )
    else:
        brnz_df.write.mode("append").saveAsTable(f"silver.{i}")


# COMMAND ----------

# DBTITLE 1,Create Gold Tables
df1 = spark.sql("select product,sum(quantity) as tot_prod_sold from silver.sales_transactions group by product order by tot_prod_sold desc")
display(df1)
df1.write.mode("overwrite").saveAsTable("gold.High_demand_table")

df2 = spark.sql(f"""select fran.supplierID , supp.name as supplier_name, count(fran.franchiseID) as no_of_franc_supplied from silver.sales_suppliers supp join silver.sales_franchises fran on 
supp.supplierID == fran.supplierID group by fran.supplierID,supp.name order by no_of_franc_supplied desc""")
display(df2)
df2.write.mode("overwrite").saveAsTable("gold.franchises_supplied")

df3 = spark.sql("""select sum(totalPrice) as sales, MONTH(dateTime) as Month from silver.sales_transactions group by Month order by sales desc""")
display(df3)
df3.write.mode("overwrite").saveAsTable("gold.Total_sales_per_month")

