package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.streaming.api.scala.createTypeInformation

class StatusCollectorStreamlet extends FlinkStreamlet {

  val fileFailStatusIn: AvroInlet[FileFailStatus]       = AvroInlet("file-fail-status-in")
  val fileSuccessStatusIn: AvroInlet[FileSuccessStatus] = AvroInlet("file-success-status-in")

  val recordFailStatusIn: AvroInlet[RecordFailStatus]       = AvroInlet("record-fail-status-in")
  val recordSuccessStatusIn: AvroInlet[RecordSuccessStatus] = AvroInlet("record-success-status-in")

  val statusOut: AvroOutlet[StatusFromCollector] = AvroOutlet("status-from-collector-out")

  override val shape: StreamletShape = StreamletShape
    .withInlets(fileFailStatusIn, fileSuccessStatusIn, recordFailStatusIn, recordSuccessStatusIn)
    .withOutlets(statusOut)

  override def createLogic(): FlinkStreamletLogic = new FlinkStreamletLogic {

    override def buildExecutionGraph(): Unit = {
      val recordFailStatus = readStream(recordFailStatusIn)
        .keyBy(_.record.fileData)

      val recordSuccessStatus = readStream(recordSuccessStatusIn)
        .keyBy(_.record.fileData)

      val recordsInput = recordFailStatus
        .connect(recordSuccessStatus)
        .process(new AggregateStatusFunction)
        .keyBy(_.fileData)

      readStream(fileFailStatusIn).map { status =>
        log.warn(s"${status.fileData.name} FAIL! Errors: ${status.errors}")
      }

      val fileSuccessStatus = readStream(fileSuccessStatusIn)
        .keyBy(_.fileData)
        .connect(recordsInput)
        .process(new StatusFileFunction)

      writeStream(statusOut, fileSuccessStatus)
    }

  }

}
