package com.spark.salesforce

import java.io.{ByteArrayInputStream, IOException}
import java.util

import com.sforce.async._
import com.sforce.soap.partner.PartnerConnection
import com.sforce.ws.{ConnectionException, ConnectorConfig}

import scala.util.control.Breaks.{break, breakable}

object BulkGetAPI {

  @throws[AsyncApiException]
  @throws[ConnectionException]
  @throws[IOException]
  def main(args: Array[String]) {
    //val example: BulkExample = new BulkExample
    runSample("Account", "venkat@sbit.com", "XXXXXXXXX")
  }

  @throws[AsyncApiException]
  @throws[ConnectionException]
  @throws[IOException]
  def runSample(sobjectType: String, userName: String, password: String) {
    val query: String = "select id, name, amount from opportunity"
    val objectName = "opportunity"
    val connection: BulkConnection = getBulkConnection(userName, password, "36.0")
    val data = doBulkQuery(connection, query, objectName)
    println(data)

  }

  @throws[AsyncApiException]
  @throws[IOException]
  def doBulkQuery(bulkConnection: BulkConnection, query:String, objectName:String) : java.util.List[java.util.Map[String, String]] = {
    var records: java.util.List[java.util.Map[String, String]]= new util.ArrayList[util.Map[String, String]]();
    try {
      var job: JobInfo = new JobInfo
      job.setObject(objectName)
      job.setOperation(OperationEnum.query)
      job.setConcurrencyMode(ConcurrencyMode.Parallel)
      job.setContentType(ContentType.CSV)
      job = bulkConnection.createJob(job)
      assert(job.getId != null)
      job = bulkConnection.getJobStatus(job.getId)
      val start: Long = System.currentTimeMillis
      var info: BatchInfo = null
      val bout: ByteArrayInputStream = new ByteArrayInputStream(query.getBytes)
      info = bulkConnection.createBatchFromStream(job, bout)
      var queryResults: Array[String] = null
      breakable {
        for(i <- 0 until 10000) {
            Thread.sleep(1000)
            info = bulkConnection.getBatchInfo(job.getId, info.getId)
            if (info.getState eq BatchStateEnum.Completed) {
              val list: QueryResultList = bulkConnection.getQueryResultList(job.getId, info.getId)
              queryResults = list.getResult
              break
            }else if (info.getState eq BatchStateEnum.Failed) {
              System.out.println("-------------- failed ----------" + info)
              break
            }else {
              System.out.println("-------------- waiting ----------" + info)
            }
        }
      }
      if (queryResults != null) {
        for (resultId <- queryResults) {
          val rdr: CSVReader = new CSVReader(bulkConnection.getQueryResultStream(job.getId, info.getId, resultId))
          val resultHeader: java.util.List[String] = rdr.nextRecord
          val resultCols: Int = resultHeader.size
          var row: java.util.List[String] = null
          while ((row = rdr.nextRecord) != null && (row != null) ) {
            val resultInfo: java.util.Map[String, String] = new java.util.HashMap[String, String]
            for (i <- 0 until resultCols) {
              resultInfo.put(resultHeader.get(i), row.get(i))
            }
            records.add(resultInfo)
          }
        }
      }
    }
    catch {
      case aae: AsyncApiException => {
        aae.printStackTrace
      }
      case ie: InterruptedException => {
        ie.printStackTrace
      }
    }
    records
  }

  @throws[ConnectionException]
  @throws[AsyncApiException]
  def getBulkConnection(userName: String, password: String, version:String): BulkConnection = {
    val partnerConfig: ConnectorConfig = new ConnectorConfig
    partnerConfig.setUsername(userName)
    partnerConfig.setPassword(password)
    partnerConfig.setAuthEndpoint(s"https://login.salesforce.com/services/Soap/u/$version")
    new PartnerConnection(partnerConfig)
    val config: ConnectorConfig = new ConnectorConfig
    config.setSessionId(partnerConfig.getSessionId)
    val soapEndpoint: String = partnerConfig.getServiceEndpoint
    val apiVersion: String = "36.0"
    val restEndpoint: String = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion
    config.setRestEndpoint(restEndpoint)
    config.setCompression(true)
    config.setTraceMessage(false)
    val connection: BulkConnection = new BulkConnection(config)
    return connection
  }
}
