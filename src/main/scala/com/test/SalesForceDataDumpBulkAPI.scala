package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by seeram on 2/13/2017.
  */
object SalesForceDataDumpBulkAPI {

  setLogLevels(Level.WARN, Seq("spark", "org", "akka"))

  def main(args: Array[String]) {

    val hiveTableName = "tempHiveTable"
    val hiveTableNameFilePath = "path"
    val sc = getSparkContext("l", "salesforce testing")

    // Writing Dataset
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("hive.exec.scratchdir", "c:/test/pavan")
    // Reading Salesforce Object
    //val soql = "select id from account"
    val bulkapi = "select id, name, amount from opportunity"
    val objectName ="opportunity"
    val sfDF = sqlContext.read.format("com.spark.salesforce")
         .option("username", "venkat@sbit.com")
         .option("password", "XXXXXXXXX")
         .option("bulkapi", bulkapi)
         .option("objectName", objectName)
         .option("version", "36.0")
         .load()

    sfDF.show()
    sfDF.printSchema()

    //sfDF.write.mode(SaveMode.Overwrite).save("yourComputerLocationLocalPath")

    //sfDF.limit(0).write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
    /*sqlContext.sql(s"ALTER TABLE $hiveTableName SET TBLPROPERTIES('EXTERNAL'='TRUE')")
    sqlContext.sql(s"ALTER TABLE $hiveTableName SET LOCATION '$hiveTableNameFilePath'")*/

    /*******This is only for debugging purpose only*******/
      //sqlContext.sql(s"show create table $hiveTableName").collect().map(_.toString().replaceAllLiterally("[","").replaceAllLiterally("]","")).foreach(println)*/
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
    sc.setLocalProperty("hive.exec.scratchdir", "c:/test")
    sc.hadoopConfiguration.set("hive.exec.local.scratchdir", "c:/test");
    sc.hadoopConfiguration.set("hive.exec.scratchdir", "c:/test");
    sc
  }

  def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] = loggers.map(loggerName => {
    val logger = Logger.getLogger(loggerName)
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    loggerName -> prevLevel
  }).toMap

}
