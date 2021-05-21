package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.AvroInlet
import org.apache.flink.streaming.api.scala.createTypeInformation

class StatusCollector extends FlinkStreamlet {

  val statusFileFailIn: AvroInlet[FileStatusFail] = AvroInlet("file-status-fail-in")

  override val shape: StreamletShape = StreamletShape
    .withInlets(statusFileFailIn)

  override def createLogic(): FlinkStreamletLogic = new FlinkStreamletLogic {

    override def buildExecutionGraph(): Unit = {
      val dataPacket = readStream(statusFileFailIn)
      dataPacket.map { data =>
        log.warn(s"${data.fileData.name} FAIL! Errors: ${data.errors}")
      }
    }

  }

}
