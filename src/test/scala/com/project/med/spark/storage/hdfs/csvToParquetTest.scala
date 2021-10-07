package com.project.med.spark.storage.hdfs
import org.junit.runner.RunWith
import com.project.med.storage.hdfs.Metadata._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.text.SimpleDateFormat

@RunWith(classOf[JUnitRunner])
class csvToParquetTest extends FunSuite with DataFrameSuiteBase {
  val metadata = Seq(
    StringFieldType("A"),
    IntegerFieldType("B"),
    LongFieldType("C"),
    DateFieldType("D", Some(new SimpleDateFormat("yyyy-MM-dd"))),
    DoubleFieldType("E"),
    LongFieldType("I_UNIQ_KPI")
  )

  test("parse metadata") {
    val metadataStr = "#comment\nA|String\nB|Integer\nC|Long\nD|DATE|YYYY-MM-DD\nE|Double\nI_UNIQ_KPI|String\n#comment"
    val lineSep = "\n"
    val colSep = """\|"""
    val actual = parseMetadata(metadataStr, lineSep, colSep)
    val expected = metadata
    assert(expected == actual)
  }

  test("get table name"){
    import com.project.med.storage.hdfs.CsvToParquet.getTableName
    val path = "hdfs://datas//di/vDCRAM/file.meta"
    val actual = getTableName(path)
    val expected = "vDCRAM"
    assert(actual==expected)
  }

  test ("generate schema"){
    import com.project.med.storage.hdfs.CsvToParquet.generateSchema
    import org.apache.spark.sql.types._

    val expected = StructType(Seq(
      StructField("A",StringType),
      StructField("B",StringType),
      StructField("C",StringType),
      StructField("D", StringType),
      StructField("E", StringType),
      StructField("I_UNIQ_KPI", StringType)
    ))
    val actual = generateSchema(metadata)
    assert(actual==expected)
  }

  test ("clean dataframe"){
    import com.project.med.storage.hdfs.CsvToParquet.cleanDataframe
    import com.project.med.storage.utils.DateConverters._
    import spark.implicits._

    val df = sc.parallelize(Seq(  ("a  ", "1", "13885308", "2014-01-01", "1.2", "1"),
      ("  a ", "  3", "000014885308", "", "4,2", "2"),
      ("", "", "", "", "", "")
    )).toDF("A","B","C","D","E","I_UNIQ_KPI")
    val expected = sc.parallelize(Seq(
      ("a", Some(1L), Some(13885308L), Some("2014-01-01".toSqlDate), Some(1.2), Some(1L)),
      ("a", Some(3L), Some(14885308L), None, Some(4.2), Some(2L)),
      (null, None, None, None, None, None)
    )).toDF("a", "b", "c", "d", "e", "i_uniq_kpi")

    val actual = cleanDataframe(df, metadata)
    assertDataFrameEquals(actual,expected)
  }
}