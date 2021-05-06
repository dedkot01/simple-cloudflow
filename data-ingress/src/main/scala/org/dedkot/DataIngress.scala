package org.dedkot

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import cloudflow.akkastream._
import cloudflow.akkastream.util.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import org.dedkot.SubscriptionDataJsonSupport._

class DataIngress extends AkkaServerStreamlet {
  val out   = AvroOutlet[SubscriptionData]("out").withPartitioner(RoundRobinPartitioner)
  val shape = StreamletShape.withOutlets(out)

  override def createLogic = HttpServerLogic.default(this, out)
}
