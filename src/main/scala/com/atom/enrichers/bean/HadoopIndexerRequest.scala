package com.atom.enrichers.bean

import com.google.gson.{Gson, JsonArray, JsonObject, JsonPrimitive}
import com.atom.enrichers.util.LogHelper
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

class HadoopIndexerRequest(var interval: String = "", var _type: String = "index_hadoop",
                           var dataSource: String = Config.druidDatasource, var tsColumn: String = Config.timestampColumn,
                           var druidDimensions: List[String] = Config.druidDimensions,
                           var druidSegmentGranularity: String = Config.druidSegmentGranularity,
                           var druidQueryGranularity: String = Config.druidQueryGranularity,
                           var druidOverlordEp: String = Config.druidOverlordEp) extends LogHelper {
  def submitTask() = {
    try {
      val taskPayload = this.getTaskPayload
      logger.info(s"Submitting TaskPayload : $taskPayload to ${Config.druidOverlordEp}")
      val client = HttpClientBuilder.create.build
      val request = new HttpPost(druidOverlordEp)
      val requestEntity = new StringEntity(
        this.getTaskPayload,
        "application/json",
        "UTF-8");
      request.setEntity(requestEntity)
      val response = client.execute(request)
      val responseStr = EntityUtils.toString(response.getEntity)
      new HadoopIndexerResponse(responseStr).getResponse
    } catch {
      case e: Exception => logger.error(e); null
    }
  }

  def getTaskPayload() = {
    val gson = new Gson()
    val countMetric = new JsonObject()
    countMetric.addProperty("type", "count")
    countMetric.addProperty("name", "count")

    val payload = new JsonObject()
    payload.addProperty("type", _type)

    val spec = new JsonObject()
    val tuningConfig = new JsonObject()
    tuningConfig.addProperty("type", "hadoop")

    val ioConfig = new JsonObject()
    ioConfig.addProperty("type", "hadoop")
    val inputSpec = new JsonObject()
    inputSpec.addProperty("type", "static")
    inputSpec.addProperty("paths", Config.outputPath + "/*/*/*")
    ioConfig.add("inputSpec", inputSpec)

    val dataSchema = new JsonObject()
    dataSchema.addProperty("dataSource", dataSource)

    val parser = new JsonObject()
    parser.addProperty("type", "hadoopyString")
    val parseSpec = new JsonObject()
    parseSpec.addProperty("format", "json")
    val timestampSpec = new JsonObject()
    timestampSpec.addProperty("column", tsColumn)
    timestampSpec.addProperty("format", "auto")
    parseSpec.add("timestampSpec", timestampSpec)
    val dimensionsSpec = new JsonObject()
    val dimsArray = new JsonArray()
    for (d <- druidDimensions)
      dimsArray.add(new JsonPrimitive(d))
    dimensionsSpec.add("dimensions", dimsArray)
    dimensionsSpec.add("spatialDimensions", new JsonArray())
    dimensionsSpec.add("dimensionExclusions", new JsonArray())
    parseSpec.add("dimensionsSpec", dimensionsSpec)
    parser.add("parseSpec", parseSpec)
    dataSchema.add("parser", parser)

    val metricsArray = new JsonArray()
    metricsArray.add(countMetric)
    dataSchema.add("metricsSpec", metricsArray)

    val granularitySpec = new JsonObject()
    granularitySpec.addProperty("type", "uniform")
    granularitySpec.addProperty("segmentGranularity", druidSegmentGranularity)
    granularitySpec.addProperty("queryGranularity", druidQueryGranularity)
    val intervals = new JsonArray()
    intervals.add(new JsonPrimitive(interval))
    granularitySpec.add("intervals", intervals)
    dataSchema.add("granularitySpec", granularitySpec)

    spec.add("dataSchema", dataSchema)
    spec.add("ioConfig", ioConfig)
    spec.add("tuningConfig", tuningConfig)

    payload.add("spec", spec)

    gson.toJson(payload)
  }
}