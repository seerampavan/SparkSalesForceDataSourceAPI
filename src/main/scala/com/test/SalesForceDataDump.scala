package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by seeram on 2/13/2017.
  */
object SalesForceDataDump {

  setLogLevels(Level.WARN, Seq("spark", "org", "akka"))

  def main(args: Array[String]) {

    val hiveTableName = "tempHiveTable"
    val hiveTableNameFilePath = "path"
    val sc = getSparkContext("l", "salesforce testing")

    // Writing Dataset
    val sqlContext = new SQLContext(sc)
    // Reading Salesforce Object
    //val soql = "select id from account"
    val soql = "select id, name, amount from opportunity"
    val sfDF = sqlContext.
       read.
       format("com.spark.salesforce").
       option("username", "venkat@sbit.com").
       option("password", "XXXXXX").
       option("soql", soql).
       option("version", "36.0").
       load()

    sfDF.show()
    sfDF.printSchema()

    /*sfDF.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
    sqlContext.sql(s"ALTER TABLE $hiveTableName SET TBLPROPERTIES('EXTERNAL'='TRUE')")
    sqlContext.sql(s"ALTER TABLE $hiveTableName SET LOCATION '$hiveTableNameFilePath'")

    /*******This is only for debugging purpose only*******/
    sqlContext.sql("show create table tempHiveTable").collect().map(_.toString().replaceAllLiterally("[","").replaceAllLiterally("]","")).foreach(println)*/

    // Reading Dataset
    /*val saql = "q = load \"<dataset_id>/<dataset_version_id>\"; q = foreach q generate  'Name' as 'Name',  'Email' as 'Email';"
    val sfWaveDF = sqlContext.
      read.
      format("com.springml.spark.salesforce").
      option("username", "venkat@sbit.com").
      option("password", "XXXXXX").
      option("saql", saql).
      option("inferSchema", "true").
      load()

    sfWaveDF.show()
    sfWaveDF.printSchema()*/

    // Update Salesforce Object
    // CSV should contain Id column followed other fields to be Updated
    // Sample -
    // Id,Description
    // 003B00000067Rnx,Superman
    // 003B00000067Rnw,SpiderMan
    /*val df = sqlContext.
      read.
      format("com.databricks.spark.csv").
      option("header", "true").
      load("data")

    df.write.
      format("com.springml.spark.salesforce").
      option("username", "venkat@sbit.com").
      option("password", "XXXXXX").
      option("sfObject", "Contact").
      save()*/
  }

  def getSparkContext(runLocal: String, appName: String) = {

    val sc: SparkContext = if (runLocal.equalsIgnoreCase("local") || runLocal.equalsIgnoreCase("l")) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[1]", appName, sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName(appName)
      new SparkContext(sparkConfig)
    }
    sc.hadoopConfiguration.setBoolean("parquet.enable.summary-metadata", false)
    sc
  }

  def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] = loggers.map(loggerName => {
    val logger = Logger.getLogger(loggerName)
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    loggerName -> prevLevel
  }).toMap

}
