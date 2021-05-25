package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.AvroInlet
import org.apache.flink.streaming.api.scala.createTypeInformation

class StatusCollector extends FlinkStreamlet {

  val fileFailStatusIn: AvroInlet[FileFailStatus] = AvroInlet("file-fail-status-in")

  val recordFailStatusIn: AvroInlet[RecordFailStatus]       = AvroInlet("record-fail-status-in")
  val recordSuccessStatusIn: AvroInlet[RecordSuccessStatus] = AvroInlet("record-success-status-in")

  override val shape: StreamletShape = StreamletShape
    .withInlets(fileFailStatusIn, recordSuccessStatusIn, recordFailStatusIn)

  override def createLogic(): FlinkStreamletLogic = new FlinkStreamletLogic {

    override def buildExecutionGraph(): Unit = {
      val fileFailStatus = readStream(fileFailStatusIn)
      fileFailStatus.map { status =>
        log.warn(s"${status.fileData.name} FAIL! Errors: ${status.errors}")
      }

      val recordSuccessStatus = readStream(recordSuccessStatusIn)
      recordSuccessStatus.map { status =>
        log.info("GOOD")
      }

      val recordFailStatus = readStream(recordFailStatusIn)
      recordFailStatus.map { status =>
        log.warn(s"${status.errors}")
      }
    }

  }

}
