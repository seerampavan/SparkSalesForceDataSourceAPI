# Spark Datasource API for salesforce  

## reading data from salesforce using spark with both BulkAPI and RESTAPI approach

    // Reading Salesforce Object
    val soql = "select id, name, amount from opportunity"
    val sfDF = sqlContext.read.
       format("com.spark.salesforce").
       option("username", "venkat@sbit.com").
       option("password", "XXXXXX").
       option("soql", soql).
       option("version", "36.0").
       load()

    sfDF.show()
    sfDF.printSchema()
    
    sfDF.write.mode(SaveMode.Overwrite).save("yourComputerLocationLocalPath")
    
    // To save a internal table in hive warehouse 
    sfDF.limit(0).write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
    // To save a external hive table
    sqlContext.sql(s"ALTER TABLE $hiveTableName SET TBLPROPERTIES('EXTERNAL'='TRUE')")
    sqlContext.sql(s"ALTER TABLE $hiveTableName SET LOCATION '$hiveTableNameFilePath'")
    
    /*******This is for debugging purpose only*******/
    sqlContext.sql(s"show create table $hiveTableName").collect().map(_.toString().replaceAllLiterally("[","").replaceAllLiterally("]","")).foreach(println)