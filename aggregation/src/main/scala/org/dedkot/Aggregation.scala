package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.api.scala.createTypeInformation

class Aggregation extends FlinkStreamlet {
  val in: AvroInlet[DataPacket]                 = AvroInlet("in")
  val out: AvroOutlet[SubscriptionDataForSpark] = AvroOutlet("out")
  override val shape: StreamletShape            = StreamletShape(in, out)

  override def createLogic: FlinkStreamletLogic = new FlinkStreamletLogic {
    override def buildExecutionGraph: Unit = {
      val dataPacket = readStream(in)
      val subscriptionDataForSpark = dataPacket.map { data =>
        log.info("In FLINK!" + data.toString)
        SubscriptionDataForSpark(
          data.subscriptionData.id,
          data.subscriptionData.startDate.toEpochDay,
          data.subscriptionData.endDate.toEpochDay,
          data.subscriptionData.duration,
          data.subscriptionData.price
        )
      }
      writeStream(out, subscriptionDataForSpark)
    }
  }
}
