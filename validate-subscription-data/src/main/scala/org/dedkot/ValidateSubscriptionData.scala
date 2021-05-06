package org.dedkot

import akka.stream.scaladsl.RunnableGraph
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro._

import java.time.LocalDate

class ValidateSubscriptionData extends AkkaStreamlet {
  val in: AvroInlet[SubscriptionData]     = AvroInlet("in")
  val valid: AvroOutlet[SubscriptionData] = AvroOutlet("valid")
  override def shape: StreamletShape      = StreamletShape(in, valid)

  override def createLogic: RunnableGraphStreamletLogic = new RunnableGraphStreamletLogic() {
    override def runnableGraph: RunnableGraph[_] =
      sourceWithCommittableContext(in).via(flow).to(committableSink(valid))

    private def flow =
      FlowWithCommittableContext[SubscriptionData].filter { subscriptionData =>
        if (isValidSubscriptionDate(subscriptionData)) {
          true
        } else {
          log.warn(s"Expected valid SubscriptionData but got: $subscriptionData")
          false
        }
      }

    def isValidSubscriptionDate(data: SubscriptionData): Boolean = { //TODO мб вынести в datamodel?
      val startDate = data.startDate
      val endDate   = data.endDate

      startDate.isBefore(LocalDate.now) &&
      startDate.isBefore(endDate) &&
      startDate.plusDays(data.duration).isEqual(endDate) &&
      data.duration >= 0 &&
      data.price >= 0
    }
  }
}
