package com.atom.enrichers

import com.atom.enrichers.bean.Config
import com.atom.enrichers.processor.{TaskInputProcessor, TaskOutputProcessor}
import com.atom.enrichers.util.LogHelper
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object Driver extends LogHelper {
  def main(args: Array[String]) {
    try {
      logger info "================================"
      logger info "Started RawStreamBatchEnricher"

      var conf: SparkConf = null
      val sc = {
        conf = new SparkConf().setAppName("RawStreamBatchEnricher")
        new SparkContext(conf)
      }
      val sqlContext = new HiveContext(sc)
      val config = Config.setConfig(conf)

      // get input dataframe from hdfs
      val inputDataFrame = TaskInputProcessor.getInputDataFrame(sc, sqlContext)
      if (inputDataFrame == null) {
        logger.error(s"No paths to process at ${Config.inputPath}")
        System.exit(-1)
      }
      // process the 2 dataframes
      val outputDataFrame = TaskOutputProcessor.process(sqlContext, inputDataFrame) // a generic process step where the input dataframe is processed
      TaskOutputProcessor.submitIndexerTask(outputDataFrame) // the processed dataframe is then ingested into druid
    } catch {
      case e: Exception => println(e); logger.error("Error : ", e)
    }
  }
}