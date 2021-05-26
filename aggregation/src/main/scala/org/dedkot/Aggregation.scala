package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.api.scala.createTypeInformation

class Aggregation extends FlinkStreamlet {
  val dataIn: AvroInlet[DataPacket]                         = AvroInlet("data-in")
  val statusFromCollectorIn: AvroInlet[StatusFromCollector] = AvroInlet("status-from-collector-in")

  val out: AvroOutlet[SubscriptionDataForSpark] = AvroOutlet("out")

  override val shape: StreamletShape = StreamletShape
    .withInlets(dataIn, statusFromCollectorIn)
    .withOutlets(out)

  override def createLogic: FlinkStreamletLogic = new FlinkStreamletLogic {
    override def buildExecutionGraph: Unit = {
      val statusFromCollector = readStream(statusFromCollectorIn)
        .map(status => log.info(s"${status.fileData.name} have good records ${status.countGoodRecords}"))

      val dataPacket = readStream(dataIn)

      val subscriptionDataForSpark =
        dataPacket
          .keyBy(_.fileData)
          .map { data =>
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
