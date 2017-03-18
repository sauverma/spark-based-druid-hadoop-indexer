package com.atom.enrichers.bean

import com.atom.enrichers.util.PrettyPrinter
import com.google.gson.Gson
import com.google.gson.annotations.Expose
import com.google.gson.GsonBuilder

class TaskStatus(val jsonStr: String) {
  var id: String = _
  var status: String = _
  var duration: Long = _
}

object HadoopIndexerResponse {
  val gson: Gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
}

class HadoopIndexerResponse(val jsonStr: String) extends PrettyPrinter {
  @Expose var task: String = _
  @Expose var status: TaskStatus = _
  def getResponse(): HadoopIndexerResponse = {
    HadoopIndexerResponse.gson.fromJson(jsonStr, classOf[HadoopIndexerResponse])
  }
  def getJsonResponse(): String = {
    HadoopIndexerResponse.gson.toJson(this)
  }
}