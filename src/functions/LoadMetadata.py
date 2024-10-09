from pyspark.sql.functions import asc

db_name = "hive_metastore"
schema_name = "metadata"

# Function to load all the metadata tables into specific dataframes
def load_metadata(spark):
    df_bronzedl = spark.read.table(db_name + "." + schema_name + ".bronzedl").filter("ActiveFlag = true").orderBy(
        asc("BLTableID"))
    df_function = spark.read.table(db_name + "." + schema_name + ".function").filter("ActiveFlag = true")
    df_golddl = spark.read.table(db_name + "." + schema_name + ".golddl")
    df_schema = spark.read.table(db_name + "." + schema_name + ".schema")
    df_silverdl = spark.read.table(db_name + "." + schema_name + ".silverdl").filter("ActiveFlag = true").orderBy(
        asc("SLTableID"))
    df_silverstage = spark.read.table(db_name + "." + schema_name + ".SilverStage").filter("ActiveFlag = true").orderBy(
        asc("StageID"))
    df_source = spark.read.table(db_name + "." + schema_name + ".source").filter("ActiveFlag = true").orderBy(
        asc("SourceID"))
    df_unfilteredworkflow = spark.read.table(db_name + "." + schema_name + ".workflow")
    df_workflow = df_unfilteredworkflow.filter("ActiveFlag = true").orderBy(asc("TaskID")).orderBy(asc("WFID"))
    final_df = (df_source.join(df_bronzedl, on='SourceID')).join(df_silverdl, on='SourceID').join(df_schema, on='SchemaID').join(df_workflow, on='BLTableID')
    return df_bronzedl, df_function, df_schema, df_silverdl, df_source, df_silverstage, df_golddl, df_workflow, final_df