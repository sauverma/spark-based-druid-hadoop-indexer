package com.atom.enrichers.processor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat

import com.atom.enrichers.bean.{Config, HadoopIndexerRequest, HadoopIndexerResponse}
import com.atom.enrichers.util.LogHelper
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat

import scala.util.Sorting

object TaskOutputProcessor extends LogHelper {
  val tsFormat = ISODateTimeFormat.dateTimeParser
  val dayFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val hourFormat = DateTimeFormat.forPattern("HH")
  val segFormat = DateTimeFormat.forPattern("yyyyMMdd/HH")
  val iso8601Format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  val colCleanser = (s: String) => {
    if (s != null)
      s.trim.toLowerCase
    s
  }

  val dayExtractor = (s: String) => {
    if (s != null && s.length == 20) dayFormat print (tsFormat.parseDateTime(s))
    else ""
  }

  val hrExtractor = (s: String) => {
    if (s != null && s.length == 20) hourFormat print (tsFormat.parseDateTime(s))
    else ""
  }

  val getSegmentInterval = () => {
    var processHrs = Config.processHours.toArray
    Sorting.quickSort(processHrs)
    var lower: String = ""
    var upper: String = ""
    if (processHrs.length == 0) ""
    if (processHrs.length == 1) {
      lower = iso8601Format print dayFormat.parseDateTime(processHrs(0).substring(0, processHrs(0).indexOf("/")))
      upper = iso8601Format print dayFormat.parseDateTime(processHrs(0).substring(0, processHrs(0).indexOf("/"))).plusDays(1)
    } else {
      lower = iso8601Format print segFormat.parseDateTime(processHrs(0))
      upper = iso8601Format print segFormat.parseDateTime(processHrs.last)
    }
    lower + '/' + upper
  }

  def submitIndexerTask(inputDf: DataFrame) {
    val interval = getSegmentInterval()
    logger.info(s"SegmentInterval : $interval")
    val request: HadoopIndexerRequest = new HadoopIndexerRequest(interval)
    val task: HadoopIndexerResponse = request.submitTask
    logger.info(s"Submitted task info : ${task.getJsonResponse} from ${Config.outputPath}")
  }

  def process(sqlContext: SQLContext, streamDf: DataFrame): DataFrame = {
    val _te = udf(dayExtractor)
    val _he = udf(hrExtractor)
    val _cc = udf(colCleanser)
    logger.info(s"RAW : ${streamDf.count}")
    var joined = streamDf
    joined = joined.withColumn("dt", _te(col("ImprTimestamp")))
      .withColumn("hr", _he(col("ImprTimestamp")))
      .drop("TelcoId")
      .withColumnRenamed("dataPartnerId", "telcoid")
      .distinct
    joined.write.partitionBy("dt", "hr").json(Config.outputPath)
    joined
  }
}