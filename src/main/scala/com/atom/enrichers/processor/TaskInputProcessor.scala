package com.atom.enrichers.processor

import com.atom.enrichers.bean.Config
import com.atom.enrichers.util.LogHelper
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object TaskInputProcessor extends LogHelper {
  def getInputDataFrame(sc: SparkContext, sqlContext: HiveContext, inpPath: String=Config.inputPath): DataFrame = {
    logger.info(s"Fetching input from inpPath for ${Config.processHours}")
    var retDfList: DataFrame = null
    for (p <- Config.processHours) {
      try {
        var path = inpPath + '/' + p
        var df = sqlContext.read.json(path)
        if (retDfList == null)
          retDfList = df
        else
          retDfList = retDfList.unionAll(df)
      } catch {
        case e: Exception => logger.error(s"Error reading path $p : $e")
      }
    }
    retDfList
  }
}