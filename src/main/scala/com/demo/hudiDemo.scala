package com.demo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.log4j.{LogManager, Logger}
import scala.util.{Try,Success,Failure}
import java.io.IOException
import com.demo.{Configuration => AppConfig}
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode

object HudiDemo extends App with SparkSessionWrapper{

  val log:Logger = LogManager.getRootLogger()
  log.warn(AppConfig.toString())

  /* setup
    Setup table name, base path and a data generator to generate records for this guide.
  */
  log.warn("Setup")
  val tableName = "hudi_trips_cow"
  val basePath = AppConfig.getDataRoot() + tableName
  val dataGen = new DataGenerator
  val spark: SparkSession = getSparkSession()

  /* insert data
    Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.
  */
  log.warn("Generating data inserts")
  val inserts = convertToStringList(dataGen.generateInserts(10))
  val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
  
  log.warn(s"Writing to table $tableName at path $basePath")
  df.write.format("hudi")
    .options(getQuickstartWriteConfigs())
    .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
    .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
    .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
    .option(TABLE_NAME, tableName)
    .mode(SaveMode.Overwrite)
    .save(basePath)

  
  //   Query the data
  // This query provides snapshot querying of the ingested data. Since our partition path (region/country/city) 
  // is 3 levels nested from base path we ve used load(basePath + "/*/*/*/*").
  //  Refer to Table types and queries for more info on all table types and query types supported.
  
  log.warn(s"Query the data")
  val tripsSnapshotDF = spark
    .read
    .format("hudi")
    .load(basePath + "/*/*/*/*")
  //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
  tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

  spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
  spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

  // Update
  // This is similar to inserting new data. Generate updates to existing trips using the data generator, load into a DataFrame and write DataFrame into the hudi table.
  log.warn(s"Update the data")
  val updates = convertToStringList(dataGen.generateUpdates(10))
  val df_update = spark.read.json(spark.sparkContext.parallelize(updates, 2))
  df_update.write.format("hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
    .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
    .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
    .option(TABLE_NAME, tableName)
    .mode(SaveMode.Append)
    .save(basePath)

  log.warn(s"Spark session end")
  spark.stop()



}