package org.dedkot

import org.apache.flink.api.common.state.{ ValueState, ValueStateDescriptor }
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

class StatusFileFunction
    extends KeyedCoProcessFunction[FileData, FileSuccessStatus, RecordStatus, StatusFromCollector] {

  lazy val fileStatus: ValueState[FileSuccessStatus] = getRuntimeContext.getState(
    new ValueStateDescriptor[FileSuccessStatus]("file status", classOf[FileSuccessStatus])
  )

  lazy val counterAllRecords: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("counter records", classOf[Long])
  )
  lazy val counterValidRecords: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("counter valid records", classOf[Long])
  )

  override def processElement1(
    value: FileSuccessStatus,
    ctx: KeyedCoProcessFunction[FileData, FileSuccessStatus, RecordStatus, StatusFromCollector]#Context,
    out: Collector[StatusFromCollector]
  ): Unit = {
    fileStatus.update(value)

    val currentCounter = counterAllRecords.value()
    if (currentCounter != null && fileStatus.value().countRecord == currentCounter) {
      out.collect(StatusFromCollector(ctx.getCurrentKey, counterValidRecords.value()))

      fileStatus.clear()
      counterAllRecords.clear()
      counterValidRecords.clear()
    }
  }

  override def processElement2(
    value: RecordStatus,
    ctx: KeyedCoProcessFunction[FileData, FileSuccessStatus, RecordStatus, StatusFromCollector]#Context,
    out: Collector[StatusFromCollector]
  ): Unit = {
    val currentCounter = counterAllRecords.value()
    if (currentCounter == null) {
      counterAllRecords.update(1)
    } else {
      counterAllRecords.update(currentCounter + 1)
    }

    if (counterValidRecords.value() == null) {
      counterValidRecords.update(0)
    }
    if (value.maybeSuccess.isDefined) {
      counterValidRecords.update(counterValidRecords.value() + 1)
    }

    if (fileStatus.value() != null && fileStatus.value().countRecord == counterAllRecords.value()) {
      out.collect(StatusFromCollector(ctx.getCurrentKey, counterValidRecords.value()))

      fileStatus.clear()
      counterAllRecords.clear()
      counterValidRecords.clear()
    }

  }

}
