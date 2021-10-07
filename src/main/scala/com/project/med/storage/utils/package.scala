package com.project.med.storage


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import com.project.med.storage.utils

import java.text.SimpleDateFormat
package object utils {

  def currentDate:String = {
    import java.text.SimpleDateFormat
    import java.util.Date
    val date = new Date()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(date)
  }
//verifier new Path dans delete
  def isTableSuccess(outputFolder : String, tableName : String)(implicit fs: org.apache.hadoop.fs.FileSystem) : Boolean = {
    val successFilePath = Seq (outputFolder,tableName, currentDate,"_SUCCESS").mkString("/")
    val tableIsSuccessfull = fs.exists(new Path(successFilePath))
    if (!tableIsSuccessfull){
      fs.delete(new Path(Seq(outputFolder,tableName,currentDate).mkString("/")),true)
    }
    tableIsSuccessfull
  }

  implicit class StringOps(str: String) {
    def smartTrim: Option[String] = Option(str).map(_.trim)
  }

  def writeToParquet(maybeDf:Option[DataFrame], outputFolder: String, tableName:String):Unit = {
    maybeDf.foreach {
      _.write
        .mode("overwrite")
        .parquet(Seq(outputFolder, tableName, currentDate).mkString("/"))
    }
  }

  def writeToParquet(df:DataFrame,outputFolder : String, tableName: String) = {
    df.write
      .mode("overwrite")
      .parquet(Seq(outputFolder,tableName,currentDate).mkString("/"))
  }

  def loadParquet(tableName: String)(implicit spark : SparkSession, inputFolder: InputFolder) : DataFrame = {
    val fs = FileSystem.get(new Configuration())
    val fileStatus = fs.globStatus(new Path(Seq(inputFolder.value, tableName,"*","_SUCCESS").mkString("/")))

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val mostRecentDate = fileStatus
      .map(_.getPath.toString.split("/").takeRight(2).head)
      .map(dateFormat.parse)
      .max
    spark.read
      .parquet(Seq(inputFolder.value, tableName, dateFormat.format(mostRecentDate)).mkString("/"))
  }

}
