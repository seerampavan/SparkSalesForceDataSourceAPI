package com.spark.salesforce.metadata

import org.apache.spark.sql.types.{StructType, StructField}

/**
 * Utility to construct Metadata
 */
object MetadataConstructor {

  private def fieldJson(field: StructField, datasetName: String, dataTypeMap: Map[String, Map[String, String]]) = {
    val fieldName = field.name
    val qualifiedName = datasetName + "." + fieldName
    val sfDataType = field.dataType.typeName
    val sfTypeJson = typeJson(sfDataType, dataTypeMap)

    s"""{
     "description": "",
      "fullyQualifiedName": "$qualifiedName",
      "label": "$fieldName",
      "name": "$fieldName",
      "isSystemField": false,
      "isUniqueId": false,
      "isMultiValue": false,
      $sfTypeJson
    } """
  }

  private def typeJson(dfDataType: String, dataTypeMap: Map[String, Map[String, String]]):String = {
    // For date, it is not possible to get the Format.
    // So it is considered as String.
    // User may update the metadata in Salesforce wave
    val someSFDataType = dataTypeMap.get(dfDataType)
    var sfDataType = Map ("wave_type" -> "Text")
    if (someSFDataType.isDefined) {
      sfDataType = someSFDataType.get
    }

    val waveType = sfDataType.get("wave_type").get
    var typeJson = s""""type": "$waveType""""

    val precision = sfDataType.get("precision")
    val scale = sfDataType.get("scale")
    val format = sfDataType.get("format")
    val default = sfDataType.get("defaultValue")
    val canTruncateValue = sfDataType.get("canTruncateValue")

    if (precision.isDefined) {
      val precisionVal = precision.get
      typeJson += ",\n"
      typeJson += s""""precision": $precisionVal"""

      var defaultValue = "0"
      if (default.isDefined) {
        defaultValue = default.get
      }
      typeJson += ",\n"
      typeJson += s""""defaultValue": $defaultValue"""

      var scaleVal = "0"
      if (scale.isDefined) {
        scaleVal = scale.get
        if (scaleVal.toInt > 6) {
          sys.error("Salesforce Wave does not support scale beyond 6. Please re-run with scale <= 6")
        }
      }
      typeJson += ",\n"
      typeJson += s""""scale": $scaleVal"""
    }

    if (format.isDefined) {
      val formatVal = format.get
      typeJson += ",\n"
      typeJson += s""""format": "$formatVal""""
    }

    if (canTruncateValue.isDefined) {
      val canTruncateValueVal = canTruncateValue.get
      typeJson += ",\n"
      typeJson += s""""canTruncateValueVal": "$canTruncateValueVal""""
    }

    typeJson
  }

  def generateMetaString(schema: StructType, datasetName: String, dataTypeMap: Map[String, Map[String, String]]):String = {
    val beginJsonString =
      s"""
        |{
        |"fileFormat": {
        |"charsetName": "UTF-8",
        |"fieldsDelimitedBy": ",",
        |"numberOfLinesToIgnore": 0
        |},
        |"objects": [
        |{
        |"connector": "AcmeCSVConnector",
        |"description": "",
        |"fullyQualifiedName": "$datasetName",
        |"label": "$datasetName",
        |"name": "$datasetName",
      """.stripMargin

    val fieldsJson = schema.fields.map(field => fieldJson(field, datasetName, dataTypeMap)).mkString(",")

    val finalJson = beginJsonString+"""  "fields":[  """+ fieldsJson+"]"+"}]}"

    finalJson
  }

}