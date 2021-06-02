package org.dedkot

import cats.effect._
import cats.effect.unsafe.implicits.global
import cloudflow.spark.sql.SQLImplicits._
import cloudflow.spark.{ SparkStreamlet, SparkStreamletLogic, StreamletQueryExecution }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.AvroInlet
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import org.apache.spark.sql.Dataset

class DataStore extends SparkStreamlet {
  val in: AvroInlet[ListSubscriptionData] = AvroInlet("in")
  override val shape: StreamletShape      = StreamletShape(in)

  override def createLogic: SparkStreamletLogic = new SparkStreamletLogic() {
    override def buildStreamingQueries: StreamletQueryExecution = {
      val subscriptionDataMoreYear = readStream(in)
        .filter(item => item.isMoreYear)
        .flatMap(item => for (data <- item.list) yield data)

      val subscriptionDataLessYear = readStream(in)
        .filter(item => !item.isMoreYear)
        .flatMap(item => for (data <- item.list) yield data)

      subscriptionDataMoreYear.writeStream.foreachBatch { (batch: Dataset[SubscriptionDataForSpark], batchId: Long) =>
        batch.foreach { data =>
          insertData(data, "subscription")
          log.info(s"Save in subscription: $data")
        }
      }.start().toQueryExecution

      subscriptionDataLessYear.writeStream.foreachBatch { (batch: Dataset[SubscriptionDataForSpark], batchId: Long) =>
        batch.foreach { data =>
          insertData(data, "longSubscription")
          log.info(s"Save in longSubscription: $data")
        }
      }.start().toQueryExecution
    }

    def xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/postgres",
      "postgres", // user
      "postgres"  // password
    )

    def insertData(data: SubscriptionDataForSpark, tableName: String): Unit =
      sql"""
      INSERT INTO "$tableName" ("startDate", "endDate", "duration", "price") VALUES
      (${data.startDate}, ${data.endDate}, ${data.duration}, ${data.price})
    """.update.run.transact(xa).unsafeRunSync
  }
}
