package com.bigdata.recommender.algh.common

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.{Properties, ResourceBundle}


object ConfigUtils {

  def getValueByKey(key: String): String ={

    val value: String = ResourceBundle.getBundle("config").getString(key)

    value
  }

  def getValueByKeyFromConf(config: String, key:String): String ={

    val properties = new Properties()

    val input : InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(config)

    val reader = new BufferedReader(new InputStreamReader(input))

    properties.load(reader)

    properties.getProperty(key)
  }

}
