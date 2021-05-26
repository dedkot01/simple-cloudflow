package org.dedkot

import cloudflow.flink.{ FlinkStreamlet, FlinkStreamletLogic }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.{ AvroInlet, AvroOutlet }
import org.apache.flink.api.common.state.{ ValueState, ValueStateDescriptor }
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class StatusCollector extends FlinkStreamlet {

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

      val recordsInputCount = recordFailStatus
        .connect(recordSuccessStatus)
        .process(new CounterRecordsFunction)

      readStream(fileFailStatusIn).map { status =>
        log.warn(s"${status.fileData.name} FAIL! Errors: ${status.errors}")
      }

      val fileSuccessStatus = readStream(fileSuccessStatusIn)
        .keyBy(_.fileData)
        .connect(recordsInputCount)
        .process(new StatusFileProcessFunction)

      writeStream(statusOut, fileSuccessStatus)
    }

    class CounterRecordsFunction extends CoProcessFunction[RecordFailStatus, RecordSuccessStatus, CounterRecords] {

      lazy val counter: ValueState[CounterRecords] =
        getRuntimeContext.getState(
          new ValueStateDescriptor[CounterRecords]("saved counter", classOf[CounterRecords])
        )

      override def processElement1(
        value: RecordFailStatus,
        ctx: CoProcessFunction[RecordFailStatus, RecordSuccessStatus, CounterRecords]#Context,
        out: Collector[CounterRecords]
      ): Unit = {
        val state = Option(counter.value)
        state match {
          case None => counter.update(CounterRecords(1L, 0L))
          case Some(currentValue) =>
            counter.update(CounterRecords(currentValue.size + 1, currentValue.counterGoodRecords))
        }
        out.collect(counter.value)
      }

      override def processElement2(
        value: RecordSuccessStatus,
        ctx: CoProcessFunction[RecordFailStatus, RecordSuccessStatus, CounterRecords]#Context,
        out: Collector[CounterRecords]
      ): Unit = {
        val state = Option(counter.value)
        state match {
          case None => counter.update(CounterRecords(1L, 1L))
          case Some(currentValue) =>
            counter.update(CounterRecords(currentValue.size + 1, currentValue.counterGoodRecords + 1L))
        }
        out.collect(counter.value)
      }
    }

    class StatusFileProcessFunction extends CoProcessFunction[FileSuccessStatus, CounterRecords, StatusFromCollector] {

      lazy val fileStatus: ValueState[FileSuccessStatus] = getRuntimeContext.getState(
        new ValueStateDescriptor[FileSuccessStatus]("file status", classOf[FileSuccessStatus])
      )

      override def processElement1(
        value: FileSuccessStatus,
        ctx: CoProcessFunction[FileSuccessStatus, CounterRecords, StatusFromCollector]#Context,
        out: Collector[StatusFromCollector]
      ): Unit = {
        fileStatus.update(value)
      }

      override def processElement2(
        value: CounterRecords,
        ctx: CoProcessFunction[FileSuccessStatus, CounterRecords, StatusFromCollector]#Context,
        out: Collector[StatusFromCollector]
      ): Unit = {
        val state = Option(fileStatus.value)
        state match {
          case Some(currentState) =>
            if (currentState.countRecord == value.size) {
              out.collect(StatusFromCollector(currentState.fileData, value.counterGoodRecords))
              fileStatus.clear()
            }
        }
      }

    }
  }

}
