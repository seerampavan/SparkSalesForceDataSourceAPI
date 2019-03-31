/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spark.salesforce

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.Utils._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import com.springml.spark.salesforce.metadata.MetadataConstructor
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import com.springml.salesforce.wave.api.APIFactory
import org.apache.spark.sql.DataFrame

/**
 * Default source for Salesforce wave data source.
 * It can write given DF to Salesforce wave repository
 * It can read Salesforce wave data source using provided SAQL and construct dataframe
 * It can read Salesforce objects using provided SOQL and construct dataframe
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  private def createReturnRelation(data: DataFrame) = {

    new BaseRelation {
      override def sqlContext: SQLContext = data.sqlContext
      override def schema: StructType = data.schema
    }
  }

  /**
   * Execute the SAQL against Salesforce Wave and construct dataframe with the result
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   *
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "36.0")
    val saql = parameters.get("saql")
    val soql = parameters.get("soql")
    val bulkapi = parameters.get("bulkapi")
    val objectName = parameters.get("objectName")
    val resultVariable = parameters.get("resultVariable")
    val pageSize = parameters.getOrElse("pageSize", "1000")
    val maxRetry = parameters.getOrElse("maxRetry", "5")
    val inferSchema = parameters.getOrElse("inferSchema", "false")
    // This is only needed for Spark version 1.5.2 or lower
    // Special characters in older version of spark is not handled properly
    val encodeFields = parameters.get("encodeFields")

//    validateMutualExclusive(saql, soql, "saql", "soql")
    val inferSchemaFlag = flag(inferSchema, "inferSchema");

    if (saql.isDefined) {
      val waveAPI = APIFactory.getInstance.waveAPI(username, password, login, version)
      DatasetRelation(waveAPI, null, null, saql.get, null, schema, sqlContext,
          resultVariable, pageSize.toInt, encodeFields, inferSchemaFlag)
    } else if(soql.isDefined){
      val forceAPI = APIFactory.getInstance.forceAPI(username, password, login,
          version, Integer.getInteger(pageSize), Integer.getInteger(maxRetry))
      DatasetRelation(null, forceAPI, null, soql.get, null, schema, sqlContext,
          null, 0, encodeFields, inferSchemaFlag)
    }else{
      val bulkConnection = BulkGetAPI.getBulkConnection(username, password, version)
      DatasetRelation(null, null, bulkConnection, bulkapi.get, objectName.get, schema, sqlContext,
        null, 0, encodeFields, inferSchemaFlag)
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val datasetName = parameters.get("datasetName")
    val sfObject = parameters.get("sfObject")
    val appName = parameters.getOrElse("appName", null)
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "36.0")
    val usersMetadataConfig = parameters.get("metadataConfig")

    validateMutualExclusive(datasetName, sfObject, "datasetName", "sfObject")

    if (datasetName.isDefined) {
      logger.info("Writing dataframe into Salesforce Wave")
      /*writeInSalesforceWave(username, password, login, version,
          datasetName.get, appName, usersMetadataConfig, mode, data)*/
    } else {
      logger.info("Updating Salesforce Object")
      logger.info("Ignoring SaveMode as existing rows will be updated " + mode)
      updateSalesforceObject(username, password, login, version, sfObject.get, data)
    }

    return createReturnRelation(data)
  }

  private def updateSalesforceObject(
      username: String,
      password: String,
      login: String,
      version: String,
      sfObject: String,
      data: DataFrame) {

    val csvHeader = Utils.csvHeadder(data.schema);
    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    val bulkAPI = APIFactory.getInstance.bulkAPI(username, password, login, version)
    val writer = new SFObjectWriter(username, password, login, version, sfObject, csvHeader)
    logger.info("Writing data")
    val successfulWrite = writer.writeData(repartitionedRDD)
    logger.info(s"Writing data was successful was $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to update salesforce object")
    }

  }

  /*private def writeInSalesforceWave(
      username: String,
      password: String,
      login: String,
      version: String,
      datasetName: String,
      appName: String,
      usersMetadataConfig: Option[String],
      mode: SaveMode,
      data: DataFrame) {
    val dataWriter = new DataWriter(username, password, login, version, datasetName, appName)

    val metadataConfig = Utils.metadataConfig(usersMetadataConfig)
    val metaDataJson = MetadataConstructor.generateMetaString(data.schema, datasetName, metadataConfig)
    logger.info(s"Metadata for dataset $datasetName is $metaDataJson")
    logger.info("Uploading metadata for dataset " + datasetName)

    val writtenId = dataWriter.writeMetadata(metaDataJson, mode)
    if (!writtenId.isDefined) {
      sys.error("Unable to write metadata for dataset " + datasetName)
    }
    logger.info(s"Able to write the metadata is $writtenId")

    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.debug("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    logger.info("Writing data")
    val successfulWrite = dataWriter.writeData(repartitionedRDD, writtenId.get)
    logger.info(s"Written data successfully? $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to write data for " + datasetName)
    }

    logger.info("Committing...")
    val committed = dataWriter.commit(writtenId.get)
    logger.info(s"committing data was successful was $committed")

    if (!committed) {
      sys.error("Unable to commit data for " + datasetName)
    }
    logger.info(s"Successfully written data for dataset $datasetName ")
    println(s"Successfully written data for dataset $datasetName ")

  }*/

  private def validateMutualExclusive(opt1: Option[String], opt2: Option[String],
      opt1Name: String, opt2Name: String) {
    if ((opt1.isDefined && opt2.isDefined)) {
      sys.error(s"""Anyone '$opt1Name' or '$opt2Name' have to be specified for creating dataframe""")
    }

    if ((!opt1.isDefined && !opt2.isDefined)) {
      sys.error(s"""Either '$opt1Name' or '$opt2Name' have to be specified for creating dataframe""")
    }
  }

  private def param(parameters: Map[String, String], envName: String, paramName: String) : String = {
    val envProp = sys.env.get(envName);
    if (envProp != null && envProp.isDefined) {
      return envProp.get
    }

    parameters.getOrElse(paramName,
        sys.error(s"""Either '$envName' has to be added in environment or '$paramName' must be specified for salesforce package."""));
  }

  private def flag(paramValue: String, paramName: String) : Boolean = {
    if (paramValue == "false") {
      false
    } else if (paramValue == "true") {
      true
    } else {
      sys.error(s"""'$paramName' flag can only be true or false""")
    }
  }
}
