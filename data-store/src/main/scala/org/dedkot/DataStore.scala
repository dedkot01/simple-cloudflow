package org.dedkot

import cloudflow.spark.sql.SQLImplicits._
import cloudflow.spark.{ SparkStreamlet, SparkStreamletLogic, StreamletQueryExecution }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.AvroInlet
import org.apache.spark.sql.Dataset
import cats.effect._
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import org.dedkot.DataStore.{ insertData, insertLongData }

import scala.concurrent.ExecutionContext

class DataStore extends SparkStreamlet {
  val in: AvroInlet[ListSubscriptionData] = AvroInlet("in")
  override val shape: StreamletShape      = StreamletShape(in)

  override def createLogic: SparkStreamletLogic = new SparkStreamletLogic() {
    override def buildStreamingQueries: StreamletQueryExecution = {
      val subscriptionData = readStream(in)

      subscriptionData.writeStream.foreachBatch { (batch: Dataset[ListSubscriptionData], batchId: Long) =>
        batch.foreach { data =>
          if (data.isMoreYear) data.list.foreach(insertLongData)
          else data.list.foreach(insertData)
        }
      }.start().toQueryExecution
    }
  }
}

object DataStore {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/postgres",
    "postgres", // user
    "postgres"  // password
  )

  def insertData(data: SubscriptionDataForSpark): Unit = {
    sql"""
        INSERT INTO public."subscription" ("startDate", "endDate", duration, price) VALUES
        (${data.startDate}, ${data.endDate}, ${data.duration}, ${data.price})
      """.update.run.transact(xa).unsafeRunSync
  }

  def insertLongData(data: SubscriptionDataForSpark): Unit = {
    sql"""
        INSERT INTO public."longSubscription" ("startDate", "endDate", duration, price) VALUES
        (${data.startDate}, ${data.endDate}, ${data.duration}, ${data.price})
      """.update.run.transact(xa).unsafeRunSync
  }
}
