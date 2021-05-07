package org.dedkot

import cloudflow.spark.sql.SQLImplicits._
import cloudflow.spark.{ SparkStreamlet, SparkStreamletLogic, StreamletQueryExecution }
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro.AvroInlet
import org.apache.spark.sql.streaming.OutputMode

class DataStore extends SparkStreamlet {
  val in: AvroInlet[SubscriptionDataForSpark] = AvroInlet("in")
  override val shape: StreamletShape          = StreamletShape(in)

  override def createLogic: SparkStreamletLogic = new SparkStreamletLogic() {
    override def buildStreamingQueries: StreamletQueryExecution = {
      readStream(in).writeStream
        .format("console")
        .option("checkpointLocation", context.checkpointDir("console-egress"))
        .outputMode(OutputMode.Append())
        .start()
        .toQueryExecution
    }
  }
}
