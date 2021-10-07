package com.project.med.storage.hdfs
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import com.project.med.storage.customUDF._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.project.med.storage.utils._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, udf}

import scala.util.Try


object CsvToParquet {
  import Metadata._

  def main (args: Array[String]):Unit = {
    require(args.length >=2)

    val arglist = args.toList
    type OptionMap = Map[Symbol,String]

    // read options arguments into a Map
    def nextOption(map : OptionMap, list : List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--input" :: value :: tail => nextOption(map ++ Map('input -> value),tail)
        case "--output" :: value :: tail => nextOption(map ++ Map('output -> value),tail)
        case  "--log" :: value :: tail => nextOption(map ++ Map('log -> value),tail)
        case "--colSep" :: value :: tail => nextOption(map ++ Map('colSep -> value), tail)
        case "--filter" :: value :: tail => nextOption(map ++ Map('filter -> value), tail)
        case "--tables" :: value :: tail => nextOption(map++ Map('tables -> value), tail)
        case entree::tail => Holder.log.error("Unknown option" + entree)
                            sys.exit()
      }
    }
    val options = nextOption(Map(),arglist)

    val inputFolder = options('input)
    val outputFolder = options('output)
    val logFolder = options('log)
    val metadataLineSep = "\n"
    val metadataColSep = """\|"""
    val dataColSep = options.getOrElse('colSep, ";")

    implicit val spark = SparkSession
      .builder()
      .appName("csv_to_parquet")
      .getOrCreate()

    implicit val sc = spark.sparkContext
    val hconf = sc.hadoopConfiguration
    implicit val fs = org.apache.hadoop.fs.FileSystem.get(hconf)

    val tables = parseTables(options.get('tables))

    val filterOpt = options.get('filter)

    val parseErrorResult = tables.toList.flatMap{ tableName =>
      val allMetadta = sc.wholeTextFiles(Set(inputFolder, tableName, "*.meta").mkString("/")).collect()
      allMetadta.map(csvToParquet(metadataLineSep,metadataColSep,dataColSep,inputFolder,outputFolder,filterOpt))
    }
    val headerErrorResult = Seq("table name", "parquet", "csv", "line in error", "error rate")
    val source = if (inputFolder.split("/").last == "*") {
      "all"
    }else{
      inputFolder.split("/").last
    }
    sc.parallelize(headerErrorResult +: parseErrorResult).map(_.mkString(";")).repartition(1).saveAsTextFile(s"hdfs//$logFolder/$currentDate/$source")
  }

  object Holder extends Serializable {
    @transient lazy val log = Logger.getLogger(getClass.getName)
  }

  def parseTables(str : Option[String]):Array[String]={
    str.map(_.split(","))
      .getOrElse(Array("*"))
  }
//take the element before the last one
  def getTableName(metadaPath : String): String = {
    metadaPath.split("/").takeRight(2).head
  }

  def generateSchema(metadata: Seq[FieldType]):StructType = {
    StructType(
      metadata.map(fieldType =>
      StructField(fieldType.fieldName, StringType,true))
    )
  }

  def r3(df : DataFrame): DataFrame = {
    df.filter(col("LINE_TYPE")==="X")
  }

  def filterDataframe(df : DataFrame, filterOpt:Option[String] ) : DataFrame = {
    filterOpt match{
      case Some("R3") => r3(df)
      case _ => df
    }
  }

  def udfDate(format : Option[SimpleDateFormat]) = udf[Option[Date],String]{ (strDate:String) =>
    try{
      format.flatMap(sdf=>
      Option(strDate).map(strd =>
      new Date(sdf.parse(strd.trim).getTime)))
    }catch{
      case e: Exception =>{
        Holder.log.error(s"Error while parsing : $strDate with format $format")
        None
      }
    }
  }

  def udfLong = udf[Option[Long],String] {(str:String) =>
    try{
      Option(str).map(_.toLong)
    }catch{
      case e: NumberFormatException =>{
        Holder.log.error(s"Error while parsing: $str to long")
        None
      }
    }
  }

  def udfDouble = udf[Option[Double],String] {(str:String) =>
    try{
      Option(str).map(_.replace(",",".").toDouble)
    }catch {
      case e:NumberFormatException =>{
        Holder.log.error(s"Error while parsing $str to double")
        None
      }
    }
  }

  def applyUDF(fieldType: FieldType,mycol : Column) : Column = {
    fieldType match {
      case stringFieldType : StringFieldType => udfTrim(mycol)
      case longFieldType: LongFieldType => udfLong(mycol)
      case dateFieldType: DateFieldType => udfDate(dateFieldType.format)(mycol)
      case integerFieldType: IntegerFieldType => udfLong(mycol)
      case doubleFieldType: DoubleFieldType => udfDouble(mycol)
    }
  }

  def cleanDataframe(df : DataFrame, metadata: Seq[FieldType]) : DataFrame = {
    def udfEmptyToNull = udf[Option[String],String]{
      s => Option(s).filterNot(_.trim.isEmpty)
    }

    val trimDf = df.select(df.columns.map(colName => udfEmptyToNull(udfTrim(col(colName))).as(colName)):_*)
    trimDf.select(metadata.map(line=>applyUDF(line,col(line.fieldName)).as(line.fieldName.toLowerCase)):_*)
  }

  def checkLineCount(inputFolder: String, outputFolder : String, tableName : String)(implicit spark: SparkSession, sc : SparkContext) = {
    val rdd = sc.textFile(Set(inputFolder,tableName,"*csv").mkString("/"))
    val rddCount = rdd.count

    implicit val dfFolder = InputFolder(value =outputFolder)
    val dfCount = loadParquet(tableName).count

    val errorPerc = Try(1 - dfCount / rddCount.toDouble).getOrElse(0.0)

    Holder.log.info(s"Row count for $tableName : $dfCount / $rddCount => ${errorPerc.toString}%")
    Seq(tableName,dfCount.toString, rddCount.toString, (rddCount - dfCount).toString, errorPerc.toString)
  }

  def csvToParquet(
                  metadataLineSep : String,
                  metadataColSep : String,
                  dataColSep : String,
                  inputFolder : String,
                  outputFolder : String,
                  filterOpt : Option[String]
                  )(implicit spark :SparkSession, sc : SparkContext, fs: org.apache.hadoop.fs.FileSystem):((String,String))=> Seq[String] = {pathAndMeta:(String,String) =>
    val path = pathAndMeta._1
    val metadataStr = pathAndMeta._2
    val tableName = getTableName(path)
    if (isTableSuccess(outputFolder, tableName)) {
      Holder.log.info(s"table already exist: $tableName")
    }else{
      Holder.log.info(s"processing $tableName")
      val metadata = parseMetadata(metadataStr,metadataLineSep,metadataColSep)
      val schema = generateSchema(metadata)

      val data = spark.read
        .option("delimiter",dataColSep)
        .option("mode","DROPMALFORMED")
        .option("escape",""""""")
        .schema(schema)
        .csv(Set(inputFolder,tableName,"*.csv").mkString("/"))

      val filteredDF = filterDataframe(data,filterOpt)
      val cleanDF = cleanDataframe(filteredDF,metadata)
      writeToParquet(cleanDF,outputFolder,tableName)
    }
    checkLineCount(inputFolder,outputFolder,tableName)
  }
}
object Metadata extends App{
  sealed trait FieldType {
    def fieldName : String
  }
  case class StringFieldType(fieldName : String) extends FieldType
  case class LongFieldType(fieldName : String) extends FieldType
  case class DateFieldType(fieldName : String, format: Option[SimpleDateFormat]) extends FieldType
  case class IntegerFieldType(fieldName : String) extends FieldType
  case class DoubleFieldType(fieldName: String) extends FieldType

  def parseMetadata(metadata: String, lineSep : String, colSep : String) = { //: Seq[FieldType]={
    metadata
      .split(lineSep)
      .filter(line => line.nonEmpty && line.head != '#')
      .toSeq
      .map(_.split(colSep)).map { line =>
      val fieldName = line(0).trim
      val fieldType = fieldName match {
        case "I_UNIQ_KPI" =>"long"
        case "I_REGRP_KPI" => "long"
        case "I_UNIQ_KPI_JURID_M" => "long"
        case "ID_RP_GA" => "long"
        case "ID_RP_PERS" => "long"
        case "ID_RP_EJ" => "long"
        case "ID_RP_PERS_LG" => "long"
        case _ => line(1).trim.toLowerCase
      }
      fieldType match {
        case "string" => StringFieldType(fieldName)
        case "integer" => IntegerFieldType(fieldName)
        case "long" => LongFieldType(fieldName)
        case "double" => DoubleFieldType(fieldName)
        case "date" =>
          val simpleDateFormat = line(2).trim match {
            case "YYYY-MM-DD" => Some( new SimpleDateFormat("yyyy-MM-dd"))
            case _ => None
          }
          DateFieldType(fieldName, simpleDateFormat)
        case _ => StringFieldType(fieldName)
      }
    }
  }
}
