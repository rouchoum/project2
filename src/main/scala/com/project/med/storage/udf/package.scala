package com.project.med.storage
import org.apache.spark.sql.functions._
import com.project.med.storage.utils._
package object customUDF {

  val udfTrim = udf[Option[String], String](_.smartTrim)
}

