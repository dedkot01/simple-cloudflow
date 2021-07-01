package org.dedkot

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

class AggregateStatusFunction
    extends KeyedCoProcessFunction[FileData, RecordFailStatus, RecordSuccessStatus, RecordStatus] {

  override def processElement1(
    value: RecordFailStatus,
    ctx: KeyedCoProcessFunction[FileData, RecordFailStatus, RecordSuccessStatus, RecordStatus]#Context,
    out: Collector[RecordStatus]
  ): Unit = {
    out.collect(RecordStatus(ctx.getCurrentKey, Some(value), None))
  }

  override def processElement2(
    value: RecordSuccessStatus,
    ctx: KeyedCoProcessFunction[FileData, RecordFailStatus, RecordSuccessStatus, RecordStatus]#Context,
    out: Collector[RecordStatus]
  ): Unit = {
    out.collect(RecordStatus(ctx.getCurrentKey, None, Some(value)))
  }

}
