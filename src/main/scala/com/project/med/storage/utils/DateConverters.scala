package com.project.med.storage.utils
import java.sql.{Date,Timestamp}
import org.joda.time.LocalDate
object DateConverters {
  implicit class LocalDateConverter(localDate: LocalDate){
    def toSqlDate : Date = new java.sql.Date(localDate.toDate.getTime)
    def tosqlTimestamp : Timestamp = new Timestamp(localDate.toDate.getTime)
  }

  implicit class DateConverter(date: Date) {
    def toSqlTimestamp: Timestamp = new java.sql.Timestamp(date.getTime)
  }

  implicit class TimestampConverter(timestamp: Timestamp) {
    def toSqlDate: Date = new Date(timestamp.getTime)
  }

  implicit class StringConverter(str: String) {
    def toSqlDate: Date = LocalDate.parse(str).toSqlDate
    def toSqlTimestamp: Timestamp = LocalDate.parse(str).tosqlTimestamp
  }
}
