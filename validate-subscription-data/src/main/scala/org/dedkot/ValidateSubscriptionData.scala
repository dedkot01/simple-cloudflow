package org.dedkot

import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._

class ValidateSubscriptionData extends AkkaStreamlet {
  val in    = AvroInlet[SubscriptionData]("in")
  val shape = StreamletShape.withInlets(in)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = sourceWithCommittableContext(in).via(flow).to(committableSink)
    def flow =
      FlowWithCommittableContext[SubscriptionData].map { subData =>
        if (subData.duration < 0) system.log.warning(s"Bad! $subData")
        else system.log.info(s"Nice! $subData")
        subData
      }
  }
}
