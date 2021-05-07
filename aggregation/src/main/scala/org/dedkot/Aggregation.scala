package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.api.scala.createTypeInformation

class Aggregation extends FlinkStreamlet {
  val in: AvroInlet[SubscriptionData]           = AvroInlet("in")
  val out: AvroOutlet[SubscriptionDataForSpark] = AvroOutlet("out")
  override val shape: StreamletShape            = StreamletShape(in, out)

  override def createLogic: FlinkStreamletLogic = new FlinkStreamletLogic {
    override def buildExecutionGraph: Unit = {
      val subscriptionData = readStream(in)
      val subscriptionDataForSpark = subscriptionData.map { data =>
        log.info("In FLINK!" + data.toString)
        SubscriptionDataForSpark(
          data.id,
          data.startDate.toEpochDay,
          data.endDate.toEpochDay,
          data.duration,
          data.price
        )
      }
      writeStream(out, subscriptionDataForSpark)
    }
  }
}
