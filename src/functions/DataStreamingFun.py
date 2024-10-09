import os
import logging
from pyspark.sql.functions import *
import shutil
from pyspark.sql import *
from time import sleep
from pyspark.sql.types import TimestampType, StringType
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import delta

# Set up logging
logging.basicConfig(level=logging.INFO)  # Set the desired logging level
logger = logging.getLogger("DataStreamingFun_Logger")

#Function to select particular columns from a dataframe
def Streaming_func_sample(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id,spark):
    logger.info("Entering Streaming_func_sample fuction")
    streaming_func_df = streaming_df.select(*col_names)
    logger.info("Exiting Streaming_func_sample fuction")
    return streaming_func_df

def trim_csv_data(streaming_df, sl_output_col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id,spark):
    logger.info("Entered trim function")
    for column_name in sl_output_col_names:
        streaming_df = streaming_df.withColumn(column_name,
                                       when(col(column_name).cast("string").isNull(), col(column_name)).otherwise(
                                           trim(col(column_name))))
    logger.info("Exiting trim function")
    return streaming_df

def validate_length(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entered validate_length function")
    final_valid_df = streaming_df.sql_ctx.createDataFrame([], streaming_df.schema)
    excep_df = spark.sql(f"select ExcepPath,ExcepTableName,ExcepCheckpoint from metadata.exception where ExcepId = (select ExcepId from metadata.silverdl where SLTableID = {SLTableID})").collect()
    ExcepPath = excep_df[0].ExcepPath
    ExcepTableName = excep_df[0].ExcepTableName
    ExcepCheckpoint = excep_df[0].ExcepCheckpoint

    for col_info in col_names:
        column, max_length = col_info.split('=')
        max_length = int(max_length)           
        final_valid_df = streaming_df.filter(length(col(column)) <= max_length)
        final_exception_df = streaming_df.filter(length(col(column)) > max_length) \
                .withColumn("BLTableID", lit(BLTableID)) \
                .withColumn("SLTableID", lit(SLTableID)) \
                .withColumn("sl_stage_func_id", lit(sl_stage_func_id)) \
                .withColumn("exception_column", lit(column)) \
                .withColumn("validated_timestamp", current_timestamp())

        try:
            query = final_exception_df.writeStream \
                .outputMode("append") \
                .format("delta") \
                .option("checkpointLocation", ExcepCheckpoint) \
                .option("path", ExcepPath) \
                .option("mergeSchema", "true") \
                .toTable(f"dev_catalog.exception.{ExcepTableName}")
            #stop streaming query using function
            query_stop_fun(query)
        except Exception as e:
            # Handle the exception
            logger.info("An error occurred:", str(e))

    if include_exception_flag:
        logger.info("Exiting validate_length function")
        return streaming_df
    else: 
        logger.info("Exiting validate_length function")
        return final_valid_df
    
#Function to generate HashKey
def hashkey_generation(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering hashkey_generation function")
    streaming_df = streaming_df.withColumn("HashKey",sha2(concat(*col_names),256))
    logger.info("Exiting hashkey_generation function")
    return streaming_df

#Function to Generate timestamp by adding date and timestamp column
def sf_timestamp_generation(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering sf_timestamp_generation function")
    #specifying seperator value as 'T'
    streaming_df = streaming_df.withColumn("sf_timestamp", concat_ws('T', *col_names))
    logger.info("Exiting sf_timestamp_generation function")
    return streaming_df

#Function to explode column data
def custom_column_explode(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering custom_column_explode function")
    all_columns = streaming_df.columns
    columns_to_explode = col_names
    final_required_columns = [item for item in all_columns if item not in columns_to_explode]

    streaming_df = streaming_df.select("*",explode(arrays_zip(*columns_to_explode)).alias("exploded_values")).select("exploded_values.*",*final_required_columns)
    logger.info("Exiting custom_column_explode function")
    return streaming_df

#Function to add T in between date and timestamp
def add_T_if_missing(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering add_T_if_missing function")
    if len(col_names) > 0:
        for column_name in col_names:
            streaming_df = streaming_df.withColumn(column_name, regexp_replace(col(column_name), " ", "T"))
    else:
        logger.info("Empty list - col_names")
    logger.info("Exiting add_T_if_missing function")
    return streaming_df

#Function to filter data based on pos system
def pos_system_filter(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering pos_system_filter function")
    streaming_df = streaming_df.filter((col(col_names[0]) == col_names[1]))
    logger.info("Exiting pos_system_filter function")
    return streaming_df

#Function to stop the Streaming functionality
def query_stop_fun(streamingDf):
    while streamingDf.isActive:
        sleep(10)
        currProgress = streamingDf.recentProgress
        if len(currProgress) > 10:
            # Get the last 5 dictionaries from the recentProgress list
            last_5_dicts = currProgress[-5:]
            # Extract the 'batchId' and 'numInputRows' values from the first dictionary
            first_dict = last_5_dicts[0]
            batchId_value = first_dict["batchId"]
            numInputRows_value = 0  # assuming no incoming records to process
            # Iterate through the remaining dictionaries and compare values
            have_same_values = all(
                d["numInputRows"] == numInputRows_value
                for d in last_5_dicts
            )
            if have_same_values:
                logger.info(f"The last 5 batches have the same values as 'numInputRows'={numInputRows_value}")
                logger.info(f"Stopping the streaming query of id {streamingDf.id}")
                streamingDf.stop()

# #Function to stop the Streaming functionality -- Old function
# def query_stop_fun(streamingDf):
#     while streamingDf.isActive:
#         sleep(10)
#         currProgress = streamingDf.recentProgress
#         if len(currProgress) > 10:
#             # Get the last 5 dictionaries from the recentProgress list
#             last_5_dicts = currProgress[-5:]
#             # Extract the 'batchId' and 'numInputRows' values from the first dictionary
#             first_dict = last_5_dicts[0]
#             batchId_value = first_dict["batchId"]
#             numInputRows_value = 0
#             # Iterate through the remaining dictionaries and compare values
#             have_same_values = all(
#                 d["batchId"] == batchId_value and d["numInputRows"] == numInputRows_value
#                 for d in last_5_dicts
#             )
#             if have_same_values:
#                 logger.info(f"The last 5 batches have the same values as 'batchId'={batchId_value} and 'numInputRows'={numInputRows_value}")
#                 logger.info(f"Stopping the streaming query of id {streamingDf.id}")
#                 streamingDf.stop()

# Define function to add seconds to the timestamp if missing
def add_seconds_to_datetime(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering add_seconds_to_datetime")
    # Registering udf in spark
    # Define a UDF to add ":00" seconds
    def add_seconds_dt(timestamp):
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M")
            return dt.strftime("%Y-%m-%dT%H:%M:00")
    spark.udf.register("add_seconds_dt_udf", add_seconds_dt, StringType())

    # Apply the UDF
    streaming_df = streaming_df.withColumn("formatted_datetime", when(col(col_names[0]).rlike("\\d{2}:\\d{2}:\\d{2}"), col(col_names[0])).otherwise(expr("add_seconds_dt_udf(" + col_names[0] + ")")))
    logger.info("Exiting add_seconds_to_datetime")
    return streaming_df


# Define function to convert timestamp from any timezone to EST with date_format as yyyy-MM-dd'T'HH:mm:ss considering daylight savings time
def datetime_to_est_with_dst(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering datetime_to_est_with_dst")
    def dt_to_est_with_dst(date_time, offset):
        try: 
            inp_date_format = "%Y-%m-%dT%H:%M:%S"
            oup_date_format = "%Y-%m-%dT%H:%M:%S"
            # Get the current time in the US/Eastern timezone
            eastern_timezone = pytz.timezone("US/Eastern")
            now = datetime.now(eastern_timezone)
            # Check if daylight savings time is in effect
            is_dst = now.dst() != timedelta(0)
            # Parse the input date and time
            input_datetime = datetime.strptime(date_time, inp_date_format)
            # Add the offset in hours
            local_datetime = input_datetime + timedelta(hours=offset)
            # Calculate the base offset for Eastern Standard Time (EST)
            base_est = -5
            if is_dst:
                base_est = -4
            # Adjust the offset
            offset += base_est
            # Create the offset-aware datetime
            local_datetime = eastern_timezone.localize(local_datetime)
            # Convert the datetime to "%Y-%m-%dT%H:%M:%S" format
            result_datetime = local_datetime.strftime(f"{oup_date_format}")
            return result_datetime
        except Exception as e:
            # Handle the exception
            logger.info("An error occurred in convert_to_est_with_dst: ", str(e))
    spark.udf.register("dt_to_est_with_dst_udf", dt_to_est_with_dst, StringType())
    # Apply the UDF - dt_to_est_with_dst_udf
    streaming_df = streaming_df.withColumn("formatted_datetime", expr("dt_to_est_with_dst_udf(" + col_names[0] + ", " + col_names[1] + ")"))
    logger.info("Exiting datetime_to_est_with_dst")
    return streaming_df

# Define function to add seconds to the timestamp if missing
def add_seconds_to_timestamp(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering add_seconds_to_timestamp")
    # Registering udf in spark
    # Define a UDF to add ":00" before the zone part (ex: -07:00, +05:30, -04:00)
    def add_seconds(timestamp):
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")
            return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M%z")
            return dt.strftime("%Y-%m-%dT%H:%M:00%z")
    spark.udf.register("add_seconds_udf", add_seconds, StringType())

    # Apply the UDF
    streaming_df = streaming_df.withColumn("formatted_timestamp", when(col(col_names[0]).rlike("\\d{2}:\\d{2}:\\d{2}"), col(col_names[0])).otherwise(expr("add_seconds_udf(" + col_names[0] + ")")))
    logger.info("Exiting add_seconds_to_timestamp")
    return streaming_df

def sum_of_col(streaming_df,col_names,BLTableID, SLTableID, include_exception_flag, sl_stage_func_id,spark):
  logger.info("Entering sum_col function")
  streaming_df=streaming_df.withColumn("summation",sum(*col_names))
  return streaming_df
# Define function to add seconds to the timestamp if missing
def add_seconds_and_remove_zone_from_timestamp(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    logger.info("Entering add_seconds_and_remove_zone_from_timestamp")
    # Registering udf in spark
    # Define a UDF to add ":00" before the zone part (ex: -07:00, +05:30, -04:00)
    def add_seconds_rem_zone(timestamp):
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M%z")
            return dt.strftime("%Y-%m-%dT%H:%M:00")
    spark.udf.register("add_seconds_rem_zone_udf", add_seconds_rem_zone, StringType())

    # Apply the UDF
    streaming_df = streaming_df.withColumn("formatted_timestamp", when(col(col_names[0]).rlike("\\d{2}:\\d{2}:\\d{2}"), col(col_names[0])).otherwise(expr("add_seconds_rem_zone_udf(" + col_names[0] + ")")))
    streaming_df = streaming_df.withColumn("formatted_timestamp", expr("substr(formatted_timestamp, 1, 19)"))
    logger.info("Exiting add_seconds_and_remove_zone_from_timestamp")
    return streaming_df

# Function to conver timestamp column containg timezone to EDT timezone timestamp
def timestamp_to_est_with_dst(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    streaming_df = streaming_df.withColumn("formatted_timestamp",
                    date_format(from_utc_timestamp(col(col_names[0]), "America/New_York"), "yyyy-MM-dd'T'HH:mm:ss"))
    return streaming_df

# Function to extract date from datetime format yyyy-MM-dd'T'HH:mm:ss into date of format yyyy-MM-dd
def extract_date(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    streaming_df = streaming_df.withColumn("formatted_date_est",date_format(col(col_names[0]), "yyyy-MM-dd"))
    return streaming_df

# Function to get count of records from a streaming dataframe
def streaming_df_count(streaming_df):
    logger.info("Entering streaming_df_count function")
    total_count = [0]
    def process_batch(df, batch_id):
        batch_count = df.count()
        total_count[0] += batch_count
    streamingDf_count_query = streaming_df.writeStream.foreachBatch(process_batch).start()
    query_stop_fun(streamingDf_count_query)
    logger.info("Exiting streaming_df_count function")
    return total_count[0]

# Function to write streaming df to table and also get count of records from a streaming dataframe
def streaming_df_write_and_count(streaming_df,checkpoint_loc,op_location,target_catalog,target_schema,table_name,spark):
    logger.info("Entering streaming_df_write_and_count function")
    # Initialize Accumulator for counting records
    # total_count = spark.sparkContext.accumulator(0)
    total_count = [0]
    def process_batch(df, batch_id):
        batch_count = df.count()
        total_count[0] += batch_count
        # total_count.add(batch_count)
        # logger.info(f"Batch ID: {batch_id}, Batch Count: {batch_count}, Total Count: {total_count.value}")
        logger.info(f"batch_id: {batch_id}, batch_count: {batch_count}, total_count: {total_count[0]}")

    # streamingDf_count_query = streaming_df.writeStream.foreachBatch(process_batch).queryName(f"streaming_count_bronze_{table_name}").start()
    # query_stop_fun(streamingDf_count_query)

    streamingDf_write_and_count_query = streaming_df.writeStream.foreachBatch(process_batch) \
                                    .format("delta") \
                                    .outputMode("append") \
                                    .queryName(f"bronze_{table_name}") \
                                    .option("checkpointLocation", checkpoint_loc) \
                                    .option("mergeSchema", "true") \
                                    .option("path",op_location) \
                                    .toTable(f"{target_catalog}.{target_schema}.bronze_{table_name}")
    query_stop_fun(streamingDf_write_and_count_query)
    logger.info("Exiting streaming_df_write_and_count function")
    return total_count[0]


# Function to insert the Incremental Data based on the ID or Datetime
def insert_audit(target_catalog,target_schema,table_name,jobID,rowCount,WatermarkColumnType,WatermarkColumnName,current_job_trig_time,table_ID,runID,spark,stage,metadata_dict):
    if stage == "Bronze":
        if WatermarkColumnType == "ID":
            new_Inserted_ID = spark.sql(f"select max({WatermarkColumnName}) from {target_catalog}.{target_schema}.bronze_{table_name}")
            nd_row = new_Inserted_ID.first()
            max_Inserted = nd_row[f'max({WatermarkColumnName})']
            # spark.sql(f"insert into dev_catalog.audit.run_stats_audit values ('{table_name}',{jobID},{rowCount},{rowCount},{max_Inserted},null,'{current_job_trig_time}',{table_ID},{runID})")
            spark.sql(f"insert into hive_metastore.audit.run_stats_audit values ({table_ID},null,null,{jobID},{runID},'{stage}','{table_name}',{rowCount},{rowCount},{max_Inserted},null,'{current_job_trig_time}',null,null,null)")
        elif WatermarkColumnType == "TimeStamp":
            new_UpdatedTime = spark.sql(f"select max({WatermarkColumnName}) as max_lastUpdatedTime from {target_catalog}.{target_schema}.bronze_{table_name}")
            nd_row = new_UpdatedTime.first()
            max_lastUpdatedTime = nd_row['max_lastUpdatedTime']
            # spark.sql(f"insert into dev_catalog.audit.run_stats_audit values ('{table_name}',{jobID},{rowCount},{rowCount},null,timestamp('{max_lastUpdatedTime}'),'{current_job_trig_time}',{table_ID},{runID})")
            spark.sql(f"insert into hive_metastore.audit.run_stats_audit values ({table_ID},null,null,{jobID},{runID},'{stage}','{table_name}',{rowCount},{rowCount},null,'{max_lastUpdatedTime}','{current_job_trig_time}',null,null,null)")
        else:
            # spark.sql(f"insert into dev_catalog.audit.run_stats_audit values ('{table_name}',{jobID},{rowCount},{rowCount},null,null,'{current_job_trig_time}',{table_ID},{runID})")
            spark.sql(f"insert into hive_metastore.audit.run_stats_audit values ({table_ID},null,null,{jobID},{runID},'{stage}','{table_name}',{rowCount},{rowCount},null,null,'{current_job_trig_time}',null,null,null)")
    elif stage == "Silver":
        sl_tableid = metadata_dict["sl_tableid"]
        spark.sql(f"insert into hive_metastore.audit.run_stats_audit values ({table_ID},{sl_tableid},null,{jobID},{runID},'{stage}','{table_name}',{rowCount},{rowCount},null,null,'{current_job_trig_time}',null,null,null)")
    elif stage == "Comparison":
        bufferedTime = metadata_dict["bufferedTime"]
        excepID = metadata_dict['excepID']
        spark.sql(f"insert into hive_metastore.audit.run_stats_audit values (null,null,{table_ID},{jobID},{runID},'{stage}','{table_name}',null,null,null,null,'{current_job_trig_time}','{current_job_trig_time}','{bufferedTime}',{excepID})")


# Function to insert the Streaming data audit record into auditcontrol.stream_data_audit_prod table
def insert_audit_stream(table_name,jobID,current_job_trig_time,table_ID,runID):
    spark.sql(f"insert into auditcontrol.stream_data_audit_prod values ('{table_name}',{jobID},'{current_job_trig_time}',{table_ID},{runID})")

# Function to get the numOutputRows count of a table from it's latest version operationMetrics
def get_rowCount(target_catalog, target_schema, table_name,spark,stage):
    stage = stage.lower()
    histDf = spark.sql(f"DESC HISTORY {target_catalog}.{target_schema}.{stage}_{table_name}")
    histDfSorted = histDf.orderBy(histDf["version"].desc()).head(1)
    if 'numOutputRows' in histDfSorted[0]['operationMetrics']:
        rowCount = int(histDfSorted[0]['operationMetrics']['numOutputRows'])
        cur_table_version = int(histDfSorted[0]['version'])
        prev_table_version = cur_table_version - 1
    else:
        rowCount = 'null'
        prev_table_version = 'null'
    return prev_table_version,rowCount

#Function to check the streaming query running continuously
def streaming_query_checker(streamingDf):
    while streamingDf.isActive:
        sleep(10)
        currProgress = streamingDf.recentProgress
        if len(currProgress) > 10:
            # Get the last 5 dictionaries from the recentProgress list
            last_5_dicts = currProgress[-5:]
            # Extract 'timestamp' values using lambda function from the last_5_dicts list
            timestamps_list = list(map(lambda d: d['timestamp'], last_5_dicts))
            # Check if all values inside the timestamps list are unique
            are_all_unique_timestamps = len(timestamps_list) == len(set(timestamps_list))
            if are_all_unique_timestamps:
                logger.info(f"The last 5 batches have unique timestamps implying that the stream is running properly for streaming query of id {streamingDf.id}")
                # Break from the loop if all timestamps are unique
                break
            
#Function to compare Raw with STD
def rawtostd_comparison(spark,rule_dict,ct,run_stats_df):
    logger.info("Entering rawtostd_comparison function")
    if rule_dict['max_lastComparedTime'] is None:
        table1_df = spark.sql(f"select * from {rule_dict['table1']} where {rule_dict['table1_timecol']} <= '{ct}'")
        table2_df = spark.sql(f"select * from {rule_dict['table2']} where {rule_dict['table2_timecol']} <= '{ct}'")
        exception_df = table1_df.join(table2_df, "HashKey", "left_anti")
        tablesn = rule_dict['table1'] + '|' + rule_dict['table2']
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting rawtostd_comparison function")
        bufferedTime = ct
        return exception_df, bufferedTime
    else:
        table1_df = spark.sql(
            f"select * from {rule_dict['table1']} where {rule_dict['table1_timecol']} <= '{ct}' and {rule_dict['table1_timecol']} >= '{rule_dict['max_lastComparedTime']}'")
        bufferedTime_df = run_stats_df.withColumn("endTime", expr(f"'{ct}' + INTERVAL {rule_dict['bufferTime']} MINUTES"))
        # display(bufferedTime_df)
        bufferedTime = bufferedTime_df.collect()[0][2]
        print(bufferedTime)
        table2_df = spark.sql(
            f"select * from {rule_dict['table2']} where {rule_dict['table2_timecol']} <= '{bufferedTime}' and {rule_dict['table2_timecol']} >= '{rule_dict['max_lastComparedBufferTime']}'")
        exception_df = table1_df.join(table2_df, "HashKey", "left_anti")
        tablesn = rule_dict['table1'] + '|' + rule_dict['table2']
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting rawtostd_comparison function")
        return exception_df,bufferedTime

#Function to compare STD with Snowflake
def stdtosf_comparison(spark,rule_dict, ct):
    logger.info("Entering stdtosf_comparison function")

    max_lastComparedTime = rule_dict['max_lastComparedTime']
    ct_date = ct.date()
    ct_buffer_date = (ct_date - timedelta(days=1))

    if max_lastComparedTime is None:
        table1_df = spark.sql(f"select * from {rule_dict['table1']} where date({rule_dict['table1_timecol']}) <= '{ct_buffer_date}'")
        table2_df = spark.sql(f"select * from {rule_dict['table2']} where date({rule_dict['table2_timecol']}) <= '{ct_date}'")

        stores_df = spark.sql("select distinct STORENUMBER from dev_catalog.metadata.stores_status where POS = 'Exploris' and LOCATIONSTATUS = 'Open'")
        
        common_stores_sf_df = table2_df.join(stores_df,table2_df.STORE_NUM==stores_df.STORENUMBER,"inner")
        sf_common_stores = common_stores_sf_df.select('STORENUMBER').distinct()
        common_stores_std_df = table1_df.join(sf_common_stores,table1_df.aapStoreId==sf_common_stores.STORENUMBER,"inner")
        std_common_df = common_stores_std_df.drop('STORENUMBER')
        sf_common_df = common_stores_sf_df.drop('STORENUMBER')
        
        exception_df = std_common_df.join(sf_common_df, "HashKey", "left_anti")
        tablesn = f"{rule_dict['table1']}|{rule_dict['table2']}"
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting stdtosf_comparison function")
        return exception_df,ct_buffer_date
    
    else:
        table1_df = spark.sql(f"select * from {rule_dict['table1']} where {rule_dict['table1_timecol']} <= '{ct_buffer_date}' and {rule_dict['table1_timecol']} > date('{rule_dict['max_lastComparedBufferTime']}') ")
        
        table2_df = spark.sql(f"select * from {rule_dict['table2']} where date({rule_dict['table2_timecol']}) <= '{ct_date}' and date({rule_dict['table2_timecol']}) > date('{rule_dict['max_lastComparedTime']}') ")

        stores_df = spark.sql("select distinct STORENUMBER from dev_catalog.metadata.stores_status where POS = 'Exploris' and LOCATIONSTATUS = 'Open'")
        
        common_stores_sf_df = table2_df.join(stores_df,table2_df.STORE_NUM==stores_df.STORENUMBER,"inner")
        sf_common_stores = common_stores_sf_df.select('STORENUMBER').distinct()
        common_stores_std_df = table1_df.join(sf_common_stores,table1_df.aapStoreId==sf_common_stores.STORENUMBER,"inner")
        std_common_df = common_stores_std_df.drop('STORENUMBER')
        sf_common_df = common_stores_sf_df.drop('STORENUMBER')

        exception_df = std_common_df.join(sf_common_df, "HashKey", "left_anti")
        tablesn = f"{rule_dict['table1']}|{rule_dict['table2']}"
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting stdtosf_comparison function")
        return exception_df, ct_buffer_date

#Function to compare Snowflake with STD
def sftostd_comparison(spark,rule_dict, ct):
    logger.info("Entering stdtosf_comparison function")
    max_lastComparedTime = rule_dict['max_lastComparedTime']
    ct_date = ct.date()
    ct_buffer_date = (ct_date - timedelta(days=1))

    if max_lastComparedTime is None:
        
        table1_df = spark.sql(f"select * from {rule_dict['table1']} where date({rule_dict['table1_timecol']}) <= '{ct_date}'")
        
        table2_df = spark.sql(f"select * from {rule_dict['table2']} where date({rule_dict['table2_timecol']}) <= '{ct_buffer_date}'")

        stores_df = spark.sql("select distinct STORENUMBER from dev_catalog.metadata.stores_status where POS = 'Exploris' and LOCATIONSTATUS = 'Open'")
        
        common_stores_sf_df = table1_df.join(stores_df,table1_df.STORE_NUM==stores_df.STORENUMBER,"inner")
        sf_common_stores = common_stores_sf_df.select('STORENUMBER').distinct()
        common_stores_std_df = table2_df.join(sf_common_stores,table2_df.aapStoreId==sf_common_stores.STORENUMBER,"inner")
        std_common_df = common_stores_std_df.drop('STORENUMBER')
        sf_common_df = common_stores_sf_df.drop('STORENUMBER')
        
        exception_df = sf_common_df.join(std_common_df, "HashKey", "left_anti")
        tablesn = f"{rule_dict['table1']}|{rule_dict['table2']}"
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting sftostd_comparison function")
        return exception_df,ct_buffer_date
    
    else:
        table1_df = spark.sql(f"select * from {rule_dict['table1']} where date({rule_dict['table1_timecol']}) <= '{ct_date}' and date({rule_dict['table1_timecol']}) > date('{rule_dict['max_lastComparedTime']}') ")
        
        table2_df = spark.sql(f"select * from {rule_dict['table2']} where {rule_dict['table2_timecol']} <= '{ct_buffer_date}' and {rule_dict['table2_timecol']} > date('{rule_dict['max_lastComparedBufferTime']}') ")

        stores_df = spark.sql("select distinct STORENUMBER from dev_catalog.metadata.stores_status where POS = 'Exploris' and LOCATIONSTATUS = 'Open'")
        
        common_stores_sf_df = table1_df.join(stores_df,table1_df.STORE_NUM==stores_df.STORENUMBER,"inner")
        sf_common_stores = common_stores_sf_df.select('STORENUMBER').distinct()
        common_stores_std_df = table2_df.join(sf_common_stores,table2_df.aapStoreId==sf_common_stores.STORENUMBER,"inner")
        std_common_df = common_stores_std_df.drop('STORENUMBER')
        sf_common_df = common_stores_sf_df.drop('STORENUMBER')

        exception_df = sf_common_df.join(std_common_df, "HashKey", "left_anti")
        tablesn = f"{rule_dict['table1']}|{rule_dict['table2']}"
        exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
            .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
            .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
            .withColumn("CreatedDTM", current_timestamp())
        logger.info("Exiting sftostd_comparison function")
        return exception_df, ct_buffer_date


def apal_date_convert(streaming_df, col_names, BLTableID, SLTableID,IncludeExceptionFlag, sl_stage_func_id, spark):
    print("***************Entering apal_date_convert function**************")
    streaming_df = streaming_df.withColumn("datefield",expr(f"to_date((substring(trim({col_names[0]}),1,8)),'yyyyMMdd')"))
    print("***************Exiting apal_date_convert function**************")
    return streaming_df

def as400_data_on_apal_date(streaming_df, col_names, BLTableID, SLTableID, sl_stage_func_id, spark):
    print("***************Entering as400_data_on_apal_date function**************")
    max_date_df = spark.sql("select max(datefield) as max_date from dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_inventory_comparision_apal")
    max_date = max_date_df.collect()[0][0]
    apal_df = spark.sql(f"select distinct StoreNum from dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_inventory_comparision_apal where datefield = '{max_date}'")
    streaming_df = streaming_df.filter(streaming_df.dgdt==max_date)
    streaming_df = streaming_df.join(apal_df,(apal_df.StoreNum==streaming_df.storenum),"inner")
    print("***************Exiting as400_data_on_apal_date function**************")
    return streaming_df


def active_sku_filter(streaming_df, col_names, BLTableID, SLTableID, IncludeExceptionFlag, sl_stage_func_id, spark):
    print("***************Entering active_sku_filter function**************")
    activesku_df = spark.sql("select SKU from onetimeload.activesku.skusales_curr_str_oh")
    streaming_df = streaming_df.join(activesku_df,"SKU","inner")
    print("***************Exiting active_sku_filter function**************")
    return streaming_df

def get_storenum_from_location(streaming_df, col_names, BLTableID, SLTableID, IncludeExceptionFlag,  sl_stage_func_id, spark):
    print("***************Entering get_storenum_from_location function**************")
    # streaming_df = spark.sql("SELECT cip.SKU AS SKU, ls.LOCATION AS LOCATION, cip.QTY_ON_HAND AS QTY_ON_HAND,cip.LAST_UPDATE_DTTM AS LAST_UPDATE_DTTM  FROM dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_current_inventory_oracle cip INNER JOIN dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_location_status_oracle ls ON cip.LOCATION_ID = ls.LOCATION_ID")
     
    #current_invDF = spark.readStream.format("delta").table("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_current_inventory_oracle")
    
    location_statusDF = spark.read.format("delta").table("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_location_status_oracle")
    streaming_df = streaming_df.join(location_statusDF,"LOCATION_ID","inner").select("SKU","QTY_ON_HAND","LAST_UPDATE_DTTM","LOCATION")
    
    #streamingDF = normalDF.writeStream.format("memory").queryName("myStream").start()
    #streaming_df2 = spark.sql("SELECT * from dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_bronze.bronze_location_status_oracle")
    print("*********get_storenum_from_location streaming_df******", streaming_df)    
    print("***************Exiting get_storenum_from_location function**************")
    return streaming_df

def apalvsoracle_comparison(spark,rule_dict,ct,dt,run_stats_df,oracle_max_time):
    print("Entering apalvsoracle_comparison function")
    exception_df = spark.sql(f"select a.StoreNum, a.SKU,int(a.Quantity) as APAL_QTY, coalesce(b.QTY_ON_HAND,0) as Oracle_QTY,a.DATETIME as APAL_DATE from {rule_dict['table1']} a left join {rule_dict['table2']} b on TRIM(a.SKU) = TRIM(b.SKU) and TRIM(a.StoreNum) = TRIM(b.LOCATION) where (coalesce((TRIM(a.Quantity)),0)) != TRIM(b.QTY_ON_HAND) and a.DATETIME like '%20240107%'")
    exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
        .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
        .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
        .withColumn("CreatedDTM", current_timestamp())
    print("Exiting apalvsoracle_comparison function")
    return exception_df

#store_number,sku,current_qty_on_hand,event_date_time
def apalvspostgres_comparison(spark,rule_dict,ct,dt,run_stats_df,oracle_max_time):
    print("Entering apalvspostgres_comparison function")
    exception_df = spark.sql(f"select a.StoreNum, a.SKU,int(a.Quantity) as APAL_QTY, coalesce(b.current_qty_on_hand,0) as Postgres_QTY,trim(CONCAT(SUBSTRING(a.DATETIME, 1, 5),'-',SUBSTRING(a.DATETIME, 6, 2),'-',SUBSTRING(DATETIME, 8, LENGTH(DATETIME) - 12))) as APAL_DATE from {rule_dict['table1']} a left join {rule_dict['table2']} b on TRIM(a.SKU) = TRIM(b.sku) and TRIM(a.StoreNum) = TRIM(b.store_number) where (coalesce((TRIM(a.Quantity)),0)) != TRIM(b.current_qty_on_hand) and a.DATETIME like '%20240107%'")
    exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
        .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
        .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
        .withColumn("CreatedDTM", current_timestamp())
    print("Exiting apalvspostgres_comparison function")
    return exception_df

#storenum,skunum,scoh,dgdt
def apalvsas400_comparison(spark,rule_dict,ct,run_stats_df,apal_max_time):
    print("Entering apalvsas400_comparison function")
    print("Apal Max Date:", apal_max_time)
    exception_df = spark.sql(f"select a.StoreNum, a.SKU,int(a.Quantity) as APAL_QTY, coalesce(int(b.scoh),0) as AS400_QTY from {rule_dict['table1']} a left join {rule_dict['table2']} b on TRIM(int(a.SKU)) = TRIM(int(b.skunum)) and TRIM(int(a.StoreNum)) = TRIM(int(b.storenum)) where (coalesce((TRIM(int(a.Quantity))),0)) != TRIM(int(b.scoh)) and a.datefield = '{apal_max_time}' and b.dgdt = '{apal_max_time}'")
    exception_df = exception_df.withColumn("RuleID", lit(rule_dict['ruleID'])) \
        .withColumn("PrimaryTable_SLTableID", lit(rule_dict['PrimaryTable_SLTableID'])) \
        .withColumn("SecondaryTable_SLTableID", lit(rule_dict['SecondaryTable_SLTableID'])) \
        .withColumn("CreatedDTM", current_timestamp())
    print("Exiting apalvsas400_comparison function")
    return exception_df   

def autoload_to_table(spark, data_source, source_format, table_name, checkpoint_directory, table_location):
    print('hello')
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("badRecordsPath", "abfss://kafka-connect-test@devdatalakelanding.dfs.core.windows.net/{}/bad-records".format(table_name)) 
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
    # )
    # query1 = query.filter(substring_index(input_file_name(), ".", -1) != source_format )
    # display(query)
    # final_query = (query1
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .option("location",table_location)
                  .table(table_name))
    return query
    #final_query
 
def Store_Sales_Transaction(gl_tablename):
    query = "SELECT aapStoreId, max(endUtcTxDateTime) AS maxEndUtcTxDateTime FROM sales_prod.SL_AAPTRXPOSSTD_tmp_vw GROUP BY  aapStoreId;"
    return query

def list_stores_max_exectime(gl_tablename, sl_table_names):
    sl_schema_name = "sales_prod"
    sl_tablename = "sl_aaptrxposstd"
    query = "SELECT aapStoreId FROM live.sl_sl_schema_name_sl_tablename_test1 GROUP BY aapStoreId"
    return query

def deduplicate(streaming_df, col_names, BLTableID, SLTableID, IncludeExceptionFlag, sl_stage_func_id, spark):
    print("***************START: deduplicate function**************")
    print("printing sl output col names===> ", col_names)
    print("*******&&&&&**********")
    print(streaming_df.columns)

    # Select only the specified columns
    streaming_func_df = streaming_df.select(*col_names)

    # Deduplicate based on the selected columns
    streaming_func_df = streaming_func_df.dropDuplicates(col_names)
    print("***********&&&&&*******")
    print(streaming_func_df.columns)
    print("***************END: deduplicate function***************")
    return streaming_func_df
    
def quantity_based_filter(streaming_df, col_names, BLTableID, SLTableID, IncludeExceptionFlag, sl_stage_func_id, spark):
    print("***************START: filterQuantityGreaterThanZero function**************")
    print("printing sl output col names ===> ", col_names)
    print("*******&&&&&**********")
    print(streaming_df.columns)

    # Ensure case consistency in column names
    # col_names = [col_name.upper() for col_name in col_names]

    # Select only the specified columns
    streaming_func_df = streaming_df.select(*col_names)

    # Filter rows where QUANTITY is greater than zero
    if BLTableID == 15:
        streaming_func_df = streaming_func_df.filter(col("scoh") > 0)
    else:
        streaming_func_df = streaming_func_df.filter(col("QUANTITY") > 0)
    print("***********&&&&&*******")
    print(streaming_func_df.columns)
    print("***************END: filterQuantityGreaterThanZero function***************")

    return streaming_func_df

def merge_inventory_data(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):

    # Create a Spark session
    # spark = SparkSession.builder.appName("MergeDeltaTable").getOrCreate()
    print("************START: merge_inventory_data************************ ")
    # Define the path to your Delta table for the target
    deltaTablePath = "abfss://az-datalake-unitycatalog@devdatalakelanding.dfs.core.windows.net/dev_catalog/inventory_silver/CORP_SC_INVENTORYMGMT_STORE_STD_Temp"
    checkpoint_location = "abfss://az-datalake-unitycatalog@devdatalakelanding.dfs.core.windows.net/dev_catalog/checkpoint/silver/CORP_SC_INVENTORYMGMT_STORE_STDS"
    
    #sl_checkpoint_loc
    #checkpoint_location = spark.sql(f"SELECT sl_checkpoint_loc FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    print("************checkpoint_location**********==>", checkpoint_location)
    #deltaTablePath = "abfss://az-datalake-unitycatalog@devdatalakelanding.dfs.core.windows.net/dev_catalog/inventory_silver/CORP_SC_INVENTORYMGMT_STORE_STD2_temp"
    #Write the streaming DataFrame to the Delta table
    print("starting temp query")
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .queryName("temp_table") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path",deltaTablePath) \
        .toTable(f"dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp")
    query_stop_fun(query)
    print("After temp query")

    #writing to another temp table
    temp2_df = spark.sql("WITH RankedInventory AS (SELECT aapStoreId,sku,eventDateTime,onHand,HashKey,ROW_NUMBER() OVER (PARTITION BY aapStoreId,sku ORDER BY eventDateTime DESC) AS rn FROM dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp) select * FROM RankedInventory WHERE rn = 1")

    # temp2_df = spark.sql("""
    #     WITH RankedInventory AS (
    #         SELECT
    #             aapStoreId,
    #             sku,
    #             eventDateTime,
    #             onHand,
    #             HashKey,
    #             ROW_NUMBER() OVER (PARTITION BY aapStoreId, sku ORDER BY eventDateTime DESC) AS rn
    #         FROM
    #             dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp
    #     )
    #     SELECT * FROM RankedInventory WHERE rn = 1
    # """)   
     
    temp2_df.write.format("delta").mode("overwrite").saveAsTable("dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp2",path="abfss://az-datalake-unitycatalog@devdatalakelanding.dfs.core.windows.net/dev_catalog/inventory_silver/CORP_SC_INVENTORYMGMT_STORE_STD2_temp2")

    # deltaTable = deltaTablePath
    # Wait for the streaming query to run and create the Delta table
    # query.awaitTermination()

    # Define the target catalog and schema
    target_catalog = "dev_catalog"
    target_schema = "inventory_silver"

    # Define the target table name using metadata
    silvertableinfo = spark.sql(f"SELECT CONCAT(target_catalog, '.', target_schema, '.', 'silver_', SLTableName) AS SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    
    sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    print(sl_table_name)

    # Check if the table exists using SHOW TABLES
    existing_tables = spark.sql("SHOW TABLES").filter("tableName = '{sl_table_name}'").count()

    # If the table does not exist, create it
    if existing_tables == 0:
        create_table_query = f"""
            CREATE TABLE {sl_table_name} (
                aapStoreId string,
                SKU string,
                OnHand int,
                eventDateTime string,
                HashKey string
            )
        """
    spark.sql(create_table_query)

    # Now, proceed with the MERGE operation
    merge_sql = f"""
        MERGE INTO {sl_table_name} AS target
        USING dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp2 AS source_data
        ON target.aapStoreId = source_data.aapStoreId
        AND target.sku = source_data.sku
        WHEN MATCHED THEN
            UPDATE SET target.OnHand = source_data.OnHand, target.eventDateTime = source_data.eventDateTime, target.HashKey = source_data.HashKey
        WHEN NOT MATCHED THEN
            INSERT (aapStoreId, SKU, OnHand, eventDateTime, Hashkey)
            VALUES (source_data.aapStoreId, source_data.sku, source_data.OnHand, source_data.eventDateTime, source_data.HashKey)
    """

    # Define the MERGE INTO SQL statement
    # merge_sql = f"""
    #     MERGE INTO {sl_table_name} AS target
    #     USING dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp2 AS source_data
    #     ON target.aapStoreId = source_data.aapStoreId
    #     AND target.sku = source_data.sku
    #     WHEN MATCHED THEN
    #         UPDATE SET target.OnHand = source_data.OnHand, target.eventDateTime = source_data.eventDateTime,target.HashKey=source_data.HashKey
    #     WHEN NOT MATCHED THEN
    #         INSERT (aapStoreId, SKU, OnHand, eventDateTime, Hashkey)
    #         VALUES (source_data.aapStoreId, source_data.sku, source_data.OnHand, source_data.eventDateTime,source_data.HashKey)
    # """

    # Execute the MERGE INTO statement
    try:
        print("entering try block")
        spark.sql(merge_sql)
        print("Merge operation completed successfully.")
        spark.sql("drop table dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp")
        spark.sql("drop table dev_catalog.inventory_silver.silver_corp_sc_inventorymgmt_store_std2_temp2")
        streaming_df = True
        return streaming_df
    except Exception as e:
        return f"Error during merge operation on '{sl_table_name}': {str(e)}"
        print("************END: merge_inventory_data************************")    
    # return streaming_df
#######################################################################################################################################################

def apal_upsert_logic(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):

    print("************START: apal_upsert_logic************************ ")
    # Define the path to your Delta table for the target
    deltaTablePath = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/inventory_comparision_apal1_Temp"
    checkpoint_location = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/checkpoint/silver/inventory_comparision_apal1"
    print("************checkpoint_location**********==>", checkpoint_location)

    #Write the streaming DataFrame to the Delta table
    print("starting temp query")
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .queryName("temp_table") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path",deltaTablePath) \
        .toTable(f"dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp")
    query_stop_fun(query)
    print("After temp query")

    # dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal

    #writing to another temp table
    # apal_temp2_df = spark.sql("WITH RankedInventory AS (SELECT StoreNum,SKU,Quantity,DATETIME,HashKey,ROW_NUMBER() OVER (PARTITION BY StoreNum,SKU ORDER BY DATETIME DESC) AS rn FROM dev_catalog.inventory_silver.silver_inventory_comparision_apal1_Temp) select * FROM RankedInventory WHERE rn = 1")

    # Writing to another temp table
    apal_temp2_df = spark.sql("""
        WITH RankedInventory AS (
            SELECT 
                StoreNum,
                SKU,
                Quantity,
                DATETIME,
                HashKey,
                ROW_NUMBER() OVER (PARTITION BY StoreNum, SKU ORDER BY DATETIME DESC) AS rn
            FROM dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp
        )
        SELECT * FROM RankedInventory WHERE rn = 1
    """)

    apal_temp2_df.write.format("delta").mode("overwrite").saveAsTable("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp2",path="abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/inventory_comparision_apal1_Temp2")

    # Define the target catalog and schema
    target_catalog = "dev_catalog_devdbxsa"
    target_schema = "dev_sc_inventory_mgmt_inventory_reconcl_silver"

    # Define the target table name using metadata
    # silvertableinfo = spark.sql(f"SELECT CONCAT(target_catalog, '.', target_schema, '.', 'silver_', SLTableName) AS SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    silvertableinfo = spark.sql(f"SELECT CONCAT('silver_',SLTableName) as SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    
    sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    print(sl_table_name)

    # Check if the table exists using SHOW TABLES
    existing_tables = spark.sql(f"show tables in {target_catalog}.{target_schema}").filter(f"tableName == '{sl_table_name}'").count()
    print(existing_tables)

    # If the table does not exist, create it
    if existing_tables == 0:
        create_table_query = f"""
            CREATE TABLE {target_catalog}.{target_schema}.{sl_table_name} (
                StoreNum string,
                SKU string,
                Quantity int,
                DATETIME string,
                HashKey string
            )
        """
        spark.sql(create_table_query)

    print("before merge operation!!!")
   # Now, proceed with the MERGE operation
    merge_sql = f"""
        MERGE INTO {target_catalog}.{target_schema}.{sl_table_name} AS target
        USING dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp2 AS source_data
        ON target.StoreNum = source_data.StoreNum
        AND target.SKU = source_data.SKU
        # WHEN MATCHED THEN
        #     UPDATE SET 
        #         target.Quantity = source_data.Quantity,
        #         target.DATETIME = source_data.DATETIME,
        #         target.HashKey = source_data.HashKey
        WHEN NOT MATCHED THEN
            INSERT (
                StoreNum, SKU, Quantity, DATETIME, HashKey
            ) VALUES (
                source_data.StoreNum, 
                source_data.SKU, 
                source_data.Quantity, 
                source_data.DATETIME, 
                source_data.HashKey
            )
    """

    print("***after merge operation*******")
    # Execute the MERGE INTO statement
    try:
        print("Entering try block!!")
        spark.sql(merge_sql)
        print("Merge operation completed successfully.")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_apal1_Temp2")
        streaming_df = True
        return streaming_df
    except Exception as e:
        return f"Error during merge operation on '{sl_table_name}': {str(e)}"
        print("************END: merge_inventory_data************************")    

########################################################################################################################################################

def as400_upsert_logic(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    print("************START: as400_upsert_logic************************ ")
    # Define the path to your Delta table for the target
    deltaTablePath = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/aap_dl_inventory_dat_daily_str_inv_Temp"
    checkpoint_location = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/checkpoint/silver/aap_dl_inventory_dat_daily_str_inv"
    print("************checkpoint_location**********==>", checkpoint_location)

    #Write the streaming DataFrame to the Delta table
    print("starting temp query")
    #filtered_df = streaming_df.filter(col("dgdt") == "2023-12-14")
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .queryName("temp_table") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path",deltaTablePath) \
        .toTable(f"dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp")
    query_stop_fun(query)
    print("After temp query")

    # dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_as400

    #writing to another temp table
    # as400_temp2_df = spark.sql("WITH RankedInventory AS (SELECT storenum,skunum,scoh,dgdt,HashKey,ROW_NUMBER() OVER (PARTITION BY storenum,skunum ORDER BY dgdt DESC) AS rn FROM dev_catalog.inventory_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp) select * FROM RankedInventory WHERE rn = 1")

    # Writing to another temp table
    as400_temp2_df = spark.sql("""
        WITH RankedInventory AS (
            SELECT 
                storenum,
                skunum,
                scoh,
                dgdt,
                HashKey,
                ROW_NUMBER() OVER (PARTITION BY storenum, skunum ORDER BY dgdt DESC) AS rn
            FROM dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp
        )
        SELECT * FROM RankedInventory WHERE rn = 1
    """)

    as400_temp2_df.write.format("delta").mode("overwrite").saveAsTable("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp2",path="abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/aap_dl_inventory_dat_daily_str_inv_Temp2")

    # Define the target catalog and schema
    target_catalog = "dev_catalog_devdbxsa"
    target_schema = "dev_sc_inventory_mgmt_inventory_reconcl_silver"

    # Define the target table name using metadata
    # silvertableinfo = spark.sql(f"SELECT CONCAT(target_catalog, '.', target_schema, '.', 'silver_', SLTableName) AS SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    silvertableinfo = spark.sql(f"SELECT CONCAT('silver_',SLTableName) as SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    
    sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    print(sl_table_name)

    # Check if the table exists using SHOW TABLES
    existing_tables = spark.sql(f"show tables in {target_catalog}.{target_schema}").filter(f"tableName == '{sl_table_name}'").count()
    print(existing_tables)

    # If the table does not exist, create it
    if existing_tables == 0:
        create_table_query = f"""
            CREATE TABLE {target_catalog}.{target_schema}.{sl_table_name} (
                storenum string,
                skunum string,
                scoh string,
                dgdt string,
                HashKey string
            )
        """
        spark.sql(create_table_query)

    print("before merge operation!!!")
   # Now, proceed with the MERGE operation
    merge_sql = f"""
        MERGE INTO {target_catalog}.{target_schema}.{sl_table_name} AS target
        USING dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp2 AS source_data
        ON target.storenum = source_data.storenum
        AND target.skunum = source_data.skunum
        WHEN MATCHED THEN
            UPDATE SET 
                target.scoh = source_data.scoh,
                target.dgdt = source_data.dgdt,
                target.HashKey = source_data.HashKey
        WHEN NOT MATCHED THEN
            INSERT (
                storenum, skunum, scoh, dgdt, HashKey
            ) VALUES (
                source_data.storenum, 
                source_data.skunum, 
                source_data.scoh, 
                source_data.dgdt, 
                source_data.HashKey
            )
    """

    print("***after merge operation*******")
    # Execute the MERGE INTO statement
    try:
        print("Entering try block!!")
        spark.sql(merge_sql)
        print("Merge operation completed successfully.")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp2")
        streaming_df = True
        return streaming_df
    except Exception as e:
        return f"Error during merge operation on '{sl_table_name}': {str(e)}"
        print("************END: merge_inventory_data************************")    


def trim_last_two_digits_and_cast_to_integer(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    print("**** Entering trim_last_two_digits_and_cast_to_integer function *****")
    streaming_df = streaming_df.withColumn(col_names[0],coalesce(expr(f"CAST(TRIM(BOTH ' ' FROM SUBSTRING({col_names[0]}, 1, LENGTH({col_names[0]}) - 2)) AS INT)"),lit(0)))
    # streaming_df = streaming_df.withColumn(col_names[0], coalesce(expr("CAST(TRIM(BOTH ' ' FROM SUBSTRING(Quantity, 1, LENGTH(Quantity) - 2)) AS INT)") )
    print("**** Exiting trim_last_two_digits_and_cast_to_integer function *****")
    return streaming_df                                 


def trim_spaces_in_cols(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    print("**** Entering trim_space_in_cols function *****")
    streaming_df=streaming_df.select(*[trim(col(column)).alias(column) for column in col_names])
    
    print("**** Discarding Null values -- Needs to be written in a seperate function*****")
    # streaming_df=streaming_df.filter(*[col(column).isNotNull() for column in col_names])
    # streaming_df_na = streaming_df.na.drop()
    
    print("**** Exiting trim_space_in_cols function *****")
    return streaming_df


# function to drop null values
def drop_nulls_in_cols(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    streaming_df_na = streaming_df.na.drop()
    return streaming_df_na 

def oracle_upsert_logic(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    print("************START: oracle_upsert_logic************************ ")
    # Define the path to your Delta table for the target
    tempTablePath = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/silver/current_inventory_oracle_temp"
    checkpoint_location = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/checkpoint/silver/current_inventory_oracle"
    print("************checkpoint_location**********==>", checkpoint_location)

    #Write the streaming DataFrame to the Delta table
    print("Oracle:: Starting temp query")
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .queryName("temp_table") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path",tempTablePath) \
        .toTable(f"dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp")
    query_stop_fun(query)
    print("After temp query!!")
    # dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_comparision_as400

    #writing to another temp table
    # as400_temp2_df = spark.sql("WITH RankedInventory AS (SELECT storenum,skunum,scoh,dgdt,HashKey,ROW_NUMBER() OVER (PARTITION BY storenum,skunum ORDER BY dgdt DESC) AS rn FROM dev_catalog.inventory_silver.silver_aap_dl_inventory_dat_daily_str_inv_Temp) select * FROM RankedInventory WHERE rn = 1")

    # Writing to another temp table
    orcl_temp2_df = spark.sql("""
        WITH RankedInventory AS (
            SELECT
                LOCATION,
                SKU,
                QTY_ON_HAND,
                LAST_UPDATE_DTTM,
                HashKey,
                ROW_NUMBER() OVER (PARTITION BY LOCATION, SKU ORDER BY LAST_UPDATE_DTTM DESC) AS rn
            FROM dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp
        )
        SELECT * FROM RankedInventory WHERE rn = 1
    """)

    orcl_temp2_df.write.format("delta").mode("overwrite").saveAsTable("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp2",path="abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/silver/inventory_reconciliation/silver_current_inventory_oracle_temp2")

    # Define the target catalog and schema
    target_catalog = "dev_catalog_devdbxsa"
    target_schema = "dev_sc_inventory_mgmt_inventory_reconcl_silver"

    # Define the target table name using metadata
    # silvertableinfo = spark.sql(f"SELECT CONCAT(target_catalog, '.', target_schema, '.', 'silver_', SLTableName) AS SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    silvertableinfo = spark.sql(f"SELECT CONCAT('silver_',SLTableName) as SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
   
    sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    print(sl_table_name)

    # Check if the table exists using SHOW TABLES
    existing_tables = spark.sql(f"show tables in {target_catalog}.{target_schema}").filter(f"tableName == '{sl_table_name}'").count()
    print(existing_tables)
    #LOCATION,SKU,QTY_ON_HAND,LAST_UPDATE_DTTM
    # If the table does not exist, create it
    if existing_tables == 0:
        create_table_query = f"""
            CREATE TABLE {target_catalog}.{target_schema}.{sl_table_name} (
                LOCATION string,
                SKU string,
                QTY_ON_HAND string,
                LAST_UPDATE_DTTM string,
                HashKey string
            )
        """
        spark.sql(create_table_query)

    print("Oracle - before merge operation!!!")
   # Now, proceed with the MERGE operation
    merge_sql = f"""
        MERGE INTO {target_catalog}.{target_schema}.{sl_table_name} AS target
        USING dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp2 AS source_data
        ON target.LOCATION = source_data.LOCATION
        AND target.SKU = source_data.SKU
        WHEN MATCHED THEN
            UPDATE SET
                target.QTY_ON_HAND = source_data.QTY_ON_HAND,
                target.LAST_UPDATE_DTTM = source_data.LAST_UPDATE_DTTM,
                target.HashKey = source_data.HashKey
        WHEN NOT MATCHED THEN
            INSERT (
                LOCATION, SKU, QTY_ON_HAND, LAST_UPDATE_DTTM, HashKey
            ) VALUES (
                source_data.LOCATION,
                source_data.SKU,
                source_data.QTY_ON_HAND,
                source_data.LAST_UPDATE_DTTM,
                source_data.HashKey
            )
    """
    print("***after merge operation*******")
    # Execute the MERGE INTO statement
    try:
        print("Entering try block!!")
        spark.sql(merge_sql)
        print("Merge operation completed successfully.")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp")
        spark.sql("drop table dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_current_inventory_oracle_temp2")
        streaming_df = True
        return streaming_df
    except Exception as e:
        return f"Error during merge operation on '{sl_table_name}': {str(e)}"
        print("************END: oracle_upsert_logic************************")  
########################################################################################################################################
def postgres_upsert_logic(streaming_df, col_names, BLTableID, SLTableID, include_exception_flag, sl_stage_func_id, spark):
    print("************START: postgres_upsert_logic************************ ")
    # Define the path to your Delta table for the target
    tempTablePath = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/silver/inventory_data_postgres_temp"
    checkpoint_location = "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/checkpoint/silver/inventory_data_postgres"
    #print("************checkpoint_location**********==>", checkpoint_location)

    silvertableinfo = spark.sql(f"SELECT CONCAT('silver_', SLTableName) as SLTableName, target_catalog, target_schema FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
 
    # Assuming you want to retrieve target_catalog, target_schema and SLTableName
    target_catalog = silvertableinfo.collect()[0]['target_catalog'].lower()
    target_schema = silvertableinfo.collect()[0]['target_schema'].lower()
    sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    
    print(f"Target Catalog: {target_catalog}")
    print(f"Target Schema: {target_schema}")
    print(f"sl_table_name: {sl_table_name}")
    print("target_catalog==>", target_catalog)

    # This code is to partially remove the hardcode of tempTablepath and checkpoint_location paths -- "abfss://../silver/"
    # Location = 'abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/inventory_reconciliation/silver/inventory_data_postgres/'
    # silvertableinfo = spark.sql(f"SELECT CONCAT('silver_', SLTableName) as SLTableName, target_catalog, target_schema, Location FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    # location = silvertableinfo.collect()[0]['Location'].lower()
    # # Extracting the desired portion of the location excluding the one before last slash
    # desired_path = '/'.join(location.split('/')[:-1]) + '/'
    # print(desired_path)    

    # Define the target catalog and schema
    # target_catalog = "dev_catalog_devdbxsa"
    # target_schema = "dev_sc_inventory_mgmt_inventory_reconcl_silver"

    #Write the streaming DataFrame to the Delta table
    print("Postgres:: starting temp query")
    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .queryName("temp_table") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path",tempTablePath) \
        .toTable(f"{target_catalog}.{target_schema}.{sl_table_name}_temp1")
    query_stop_fun(query)
    print("Postgres:: After temp query!!")

    # Writing to another temp table
    orcl_temp2_df = spark.sql(f"""
        WITH RankedInventory AS (
            SELECT
                store_number,
                sku,
                current_qty_on_hand,
                event_date_time,
                HashKey,
                ROW_NUMBER() OVER (PARTITION BY store_number, sku ORDER BY event_date_time DESC) AS rn
            FROM {target_catalog}.{target_schema}.{sl_table_name}_temp1
        )
        SELECT * FROM RankedInventory WHERE rn = 1
    """)

    # orcl_temp2_df.write.format("delta").mode("overwrite").saveAsTable("dev_catalog_devdbxsa.dev_sc_inventory_mgmt_inventory_reconcl_silver.silver_inventory_data_postgres_temp2",path="abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/silver/inventory_reconciliation/silver_inventory_data_postgres_temp2")

    orcl_temp2_df.write.format("delta").mode("overwrite").option("path", "abfss://az-datalake-unitycatalog@devdbxsa.dfs.core.windows.net/dev_catalog_devdbxsa/supply_chain/inventory_mgmt/silver/inventory_reconciliation/silver_inventory_data_postgres_temp2").saveAsTable(f"{target_catalog}.{target_schema}.{sl_table_name}_temp2")

    # Define the target table name using metadata
    # silvertableinfo = spark.sql(f"SELECT CONCAT(target_catalog, '.', target_schema, '.', 'silver_', SLTableName) AS SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
    # silvertableinfo = spark.sql(f"SELECT CONCAT('silver_',SLTableName) as SLTableName FROM hive_metastore.metadata.silverdl WHERE SLTableID = {SLTableID}")
   
    # sl_table_name = silvertableinfo.collect()[0]['SLTableName'].lower()
    # print(sl_table_name)

    # Check if the table exists using SHOW TABLES
    existing_tables = spark.sql(f"show tables in {target_catalog}.{target_schema}").filter(f"tableName == '{sl_table_name}'").count()
    print(existing_tables)

    # If the table does not exist, create it
    if existing_tables == 0:
        create_table_query = f"""
            CREATE TABLE {target_catalog}.{target_schema}.{sl_table_name} (
                store_number string,
                sku string,
                current_qty_on_hand string,
                event_date_time string,
                HashKey string
            )
        """
        spark.sql(create_table_query)

    #store_number,sku,event_date_time,current_qty_on_hand
    print("Postgres - before merge operation!!!")
    # Now, proceed with the MERGE operation
    merge_sql = f"""
        MERGE INTO {target_catalog}.{target_schema}.{sl_table_name} AS target
        USING {target_catalog}.{target_schema}.{sl_table_name}_temp2 AS source_data
        ON target.store_number = source_data.store_number
        AND target.sku = source_data.sku
        WHEN MATCHED THEN
            UPDATE SET
                target.current_qty_on_hand = source_data.current_qty_on_hand,
                target.event_date_time = source_data.event_date_time,
                target.HashKey = source_data.HashKey
        WHEN NOT MATCHED THEN
            INSERT (
                store_number, sku, current_qty_on_hand, event_date_time, HashKey
            ) VALUES (
                source_data.store_number,
                source_data.sku,
                source_data.current_qty_on_hand,
                source_data.event_date_time,
                source_data.HashKey
            )
    """

    print("***After merge operation*******")
    # Execute the MERGE INTO statement
    try:
        print("Entering try block!!")
        spark.sql(merge_sql)
        print("Postgres:: Merge operation completed successfully.")
        # spark.sql("drop table {target_catalog}.{target_schema}.{sl_table_name}_temp1")
        # spark.sql("drop table {target_catalog}.{target_schema}.{sl_table_name}_temp2")
        spark.sql(f"""DROP TABLE {target_catalog}.{target_schema}.{sl_table_name}_temp1""")
        spark.sql(f"""DROP TABLE {target_catalog}.{target_schema}.{sl_table_name}_temp2""")        
        streaming_df = True
        return streaming_df
    except Exception as e:
        return f"Postgres:: Error during merge operation on '{sl_table_name}': {str(e)}"
    print("************END:: postgres_upsert_logic************************")   

