package org.dedkot

import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.akkastream.util.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._

class ValidateSubscriptionData extends AkkaStreamlet {
  val in      = AvroInlet[SubscriptionData]("in")
  val valid   = AvroOutlet[SubscriptionData]("valid").withPartitioner(RoundRobinPartitioner)
  val invalid = AvroOutlet[InvalidSubscriptionData]("invalid").withPartitioner(RoundRobinPartitioner)
  val shape   = StreamletShape(in).withOutlets(invalid, valid)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = sourceWithCommittableContext(in).to(Splitter.sink(flow, invalid, valid))
    def flow =
      FlowWithCommittableContext[SubscriptionData].map { subData =>
        if (subData.duration < 0) Left(InvalidSubscriptionData(subData, "duration must be positive"))
        else Right(subData)
      }
  }
}
