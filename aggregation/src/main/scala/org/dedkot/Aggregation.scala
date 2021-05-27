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
      val dataPacket = readStream(dataIn).keyBy(_.fileData)
      val statusFromCollector = readStream(statusFromCollectorIn)
        .keyBy(_.fileData)
        .connect(dataPacket)
        .process(new CheckStatusFunction)

      writeStream(out, statusFromCollector)
    }

  }
}
