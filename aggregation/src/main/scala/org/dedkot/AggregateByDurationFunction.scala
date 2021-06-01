package org.dedkot

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class AggregateByDurationFunction extends KeyedProcessFunction[FileData, Seq[DataPacket], ListSubscriptionData] {

  override def processElement(
    value: Seq[DataPacket],
    ctx: KeyedProcessFunction[FileData, Seq[DataPacket], ListSubscriptionData]#Context,
    out: Collector[ListSubscriptionData]
  ): Unit = {
    val listMoreThanYear = value
      .filter(record => record.subscriptionData.duration > 365)
      .map(data => getSubscriptionDataForSpark(data.subscriptionData))
    if (listMoreThanYear.nonEmpty) out.collect(ListSubscriptionData(isMoreYear = true, listMoreThanYear))

    val listLessThanYear = value
      .filter(list => list.subscriptionData.duration <= 365)
      .map(data => getSubscriptionDataForSpark(data.subscriptionData))
    if (listLessThanYear.nonEmpty) out.collect(ListSubscriptionData(isMoreYear = false, listLessThanYear))
  }

  def getSubscriptionDataForSpark(data: SubscriptionData): SubscriptionDataForSpark = {
    SubscriptionDataForSpark(
      data.id,
      data.startDate.toEpochDay,
      data.endDate.toEpochDay,
      data.duration,
      data.price
    )
  }

}
