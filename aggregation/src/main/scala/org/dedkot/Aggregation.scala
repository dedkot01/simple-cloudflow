package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic

class Aggregation extends FlinkStreamlet {
  val dataIn: AvroInlet[DataPacket]                         = AvroInlet("data-in")
  val statusFromCollectorIn: AvroInlet[StatusFromCollector] = AvroInlet("status-from-collector-in")

  val out: AvroOutlet[ListSubscriptionData] = AvroOutlet("out")

  override val shape: StreamletShape = StreamletShape
    .withInlets(dataIn, statusFromCollectorIn)
    .withOutlets(out)

  override def createLogic: FlinkStreamletLogic = new FlinkStreamletLogic {
    context.env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    override def buildExecutionGraph: Unit = {
      val dataPacket = readStream(dataIn).keyBy(_.fileData)
      val statusFromCollector = readStream(statusFromCollectorIn)
        .keyBy(_.fileData)
        .connect(dataPacket)
        .process(new CheckStatusFunction)
      val aggregateByDate = statusFromCollector
        .keyBy(_.head.fileData)
        .process(new AggregateByDurationFunction)

      writeStream(out, aggregateByDate)
    }

  }
}
