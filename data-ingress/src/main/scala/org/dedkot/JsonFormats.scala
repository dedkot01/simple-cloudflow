package org.dedkot

import spray.json._

import java.time.LocalDate
import scala.util.Try

trait LocalDateJsonSupport extends DefaultJsonProtocol {
  implicit object LocalDateFormat extends JsonFormat[LocalDate] {
    def write(date: LocalDate) = JsString(date.toString)

    def read(json: JsValue): LocalDate = json match {
      case JsString(date) =>
        Try(LocalDate.parse(date)).getOrElse(deserializationError(s"Expected valid LocalDate, but got '$date'"))
      case other => deserializationError(s"Expected LocalDate as JsString, but got '$other''")
    }
  }
}

object SubscriptionDataJsonSupport extends DefaultJsonProtocol with LocalDateJsonSupport {
  implicit val measurementFormat = jsonFormat5(SubscriptionData.apply)
}
