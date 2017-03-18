package com.atom.enrichers.util

trait PrettyPrinter {
  override def toString = {
    var retStr = "["
    for (f <- this.getClass.getDeclaredFields) {
      f setAccessible true
      retStr += "{" + f.getName + " : " + f.get(this) + "}" + ","
    }
    retStr = retStr.substring(0, retStr.length - 1) + "]"
    retStr
  }
}
