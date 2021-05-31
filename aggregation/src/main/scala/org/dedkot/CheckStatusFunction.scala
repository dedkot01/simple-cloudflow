package org.dedkot

import org.apache.flink.api.common.state.{ ListState, ListStateDescriptor, ValueState, ValueStateDescriptor }
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration.DurationInt

class CheckStatusFunction
    extends KeyedCoProcessFunction[FileData, StatusFromCollector, DataPacket, SubscriptionDataForSpark] {

  lazy val statusFromCollector: ValueState[StatusFromCollector] = getRuntimeContext.getState(
    new ValueStateDescriptor[StatusFromCollector]("status from collector", classOf[StatusFromCollector])
  )

  lazy val counterInputRecords: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("counter input records", classOf[Long])
  )

  lazy val listData: ListState[SubscriptionDataForSpark] = getRuntimeContext.getListState(
    new ListStateDescriptor[SubscriptionDataForSpark]("list data", classOf[SubscriptionDataForSpark])
  )

  lazy val timestampLastRecord: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timestamp last record", classOf[Long])
  )

  val waitingTime: Long = 10.seconds.toMillis

  override def processElement1(
    value: StatusFromCollector,
    ctx: KeyedCoProcessFunction[FileData, StatusFromCollector, DataPacket, SubscriptionDataForSpark]#Context,
    out: Collector[SubscriptionDataForSpark]
  ): Unit = {
    statusFromCollector.update(value)
  }

  override def processElement2(
    value: DataPacket,
    ctx: KeyedCoProcessFunction[FileData, StatusFromCollector, DataPacket, SubscriptionDataForSpark]#Context,
    out: Collector[SubscriptionDataForSpark]
  ): Unit = {
    if (timestampLastRecord.value() != null)
      ctx.timerService().deleteEventTimeTimer(timestampLastRecord.value() + waitingTime)
    timestampLastRecord.update(Instant.now().toEpochMilli)
    ctx.timerService().registerEventTimeTimer(timestampLastRecord.value() + waitingTime)

    if (counterInputRecords.value() == null) counterInputRecords.update(0)

    val data = SubscriptionDataForSpark(
      value.subscriptionData.id,
      value.subscriptionData.startDate.toEpochDay,
      value.subscriptionData.endDate.toEpochDay,
      value.subscriptionData.duration,
      value.subscriptionData.price
    )
    listData.add(data)

    val currentCount = counterInputRecords.value() + 1
    counterInputRecords.update(currentCount)
    if (currentCount == statusFromCollector.value().countGoodRecords) {
      listData.get().forEach(out.collect(_))

      ctx.timerService().deleteEventTimeTimer(timestampLastRecord.value() + waitingTime)
      timestampLastRecord.clear()
      listData.clear()
      statusFromCollector.clear()
      counterInputRecords.clear()
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: KeyedCoProcessFunction[FileData, StatusFromCollector, DataPacket, SubscriptionDataForSpark]#OnTimerContext,
    out: Collector[SubscriptionDataForSpark]
  ): Unit = {
    if (Instant.now.toEpochMilli >= (timestampLastRecord.value() + waitingTime)) {
      println(s"Busy I guess ${ctx.getCurrentKey}, send ${counterInputRecords.value()} records")
      listData.get().forEach(out.collect(_))

      timestampLastRecord.clear()
      listData.clear()
      statusFromCollector.clear()
      counterInputRecords.clear()
    } else {
      ctx.timerService().registerEventTimeTimer(timestampLastRecord.value() + waitingTime)
    }
  }

}
