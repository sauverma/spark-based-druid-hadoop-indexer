package com.atom.enrichers.bean

import java.util.Date

import com.atom.enrichers.util.LogHelper
import org.apache.spark.SparkConf

object Config extends LogHelper {
  var inputPath: String = _
  var outputPath: String = _
  var processHours: List[String] = _
  var useCb: Boolean = _
  var druidDatasource: String = _
  var druidOverlordEp: String = _
  var timestampColumn: String = _
  var druidDimensions: List[String] = _
  var druidSegmentGranularity: String = _
  var druidQueryGranularity: String = _
  var impressionBasePath: String = _

  def setConfig(sparkConf: SparkConf) {
    this.inputPath = sparkConf.get("spark.enricher.inputPath", "")
    this.outputPath = sparkConf.get("spark.enricher.outputPath", "") + "/" + new Date().getTime
    this.useCb = sparkConf.getBoolean("spark.enricher.useCb", false)
    this.druidDatasource = sparkConf.get("spark.enricher.druidDatasource", "")
    this.druidOverlordEp = sparkConf.get("spark.enricher.druidOverlordEp", "")
    this.timestampColumn = sparkConf.get("spark.enricher.timestampColumn", "")
    this.druidSegmentGranularity = sparkConf.get("spark.enricher.druidSegmentGranularity", "")
    this.druidQueryGranularity = sparkConf.get("spark.enricher.druidQueryGranularity", "")
    this.impressionBasePath = sparkConf.get("spark.enricher.impressionBasePath", "")

    var hoursStr = sparkConf.get("spark.enricher.processHours", "")
    if (hoursStr == null || hoursStr.length == 0)
      this.processHours = List[String]()
    else
      this.processHours = List.fromArray[String](hoursStr.split(","))

    var druidDims = sparkConf.get("spark.enricher.druidDimensions", "")
    if (druidDims == null || druidDims.length == 0)
      this.druidDimensions = List[String]()
    else
      this.druidDimensions = List.fromArray[String](druidDims.split(","))

    var deviceIdExtractorClass = sparkConf.get("spark.enricher.deviceIdExtractorClass", "")
    if (deviceIdExtractorClass == null || deviceIdExtractorClass.length == 0)
      throw new IllegalArgumentException("spark.enricher.deviceIdExtractorClass must be non-empty")

  }
}