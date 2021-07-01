package org.dedkot

import akka.NotUsed
import akka.stream.ClosedShape
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, RunnableGraph, Source }
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro._

import java.time.LocalDate

class ValidateSubscriptionData extends AkkaStreamlet {

  val in: AvroInlet[DataPacket]        = AvroInlet("in")
  val validOut: AvroOutlet[DataPacket] = AvroOutlet("valid-out")

  val failStatusOut: AvroOutlet[RecordFailStatus]       = AvroOutlet("record-fail-status-out")
  val successStatusOut: AvroOutlet[RecordSuccessStatus] = AvroOutlet("record-success-status-out")

  override def shape: StreamletShape =
    StreamletShape
      .withInlets(in)
      .withOutlets(validOut, failStatusOut, successStatusOut)

  override def createLogic: RunnableGraphStreamletLogic = new RunnableGraphStreamletLogic() {

    override def runnableGraph: RunnableGraph[_] =
      RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val source: Source[DataPacket, NotUsed] = plainSource(in)

        val validateRecord: Flow[DataPacket, Either[RecordFailStatus, DataPacket], NotUsed] =
          Flow[DataPacket].map { dataPacket =>
            if (isValidSubscriptionDate(dataPacket.subscriptionData)) {
              Right(dataPacket)
            } else {
              Left(
                RecordFailStatus(
                  dataPacket,
                  Seq(
                    s"Expected valid SubscriptionData but got: ${dataPacket.subscriptionData} " +
                      s"from ${dataPacket.fileData.name}"
                  )
                )
              )
            }
          }

        val filterAfterValidate = builder.add(Broadcast[Either[RecordFailStatus, DataPacket]](2))
        val broadcast           = builder.add(Broadcast[DataPacket](2))

        val failStatusSink    = plainSink(failStatusOut)
        val successStatusSink = plainSink(successStatusOut)
        val validSink         = plainSink(validOut)

        source ~> validateRecord ~> filterAfterValidate.in

        filterAfterValidate.out(0).filter(_.isLeft).map(_.left.get) ~> failStatusSink
        filterAfterValidate.out(1).filter(_.isRight).map(_.right.get) ~> broadcast.in

        broadcast.out(0) ~> validSink
        broadcast.out(1).map(RecordSuccessStatus(_)) ~> successStatusSink

        ClosedShape
      })

    def isValidSubscriptionDate(data: SubscriptionData): Boolean = { //TODO мб вынести в datamodel?
      val startDate = data.startDate
      val endDate   = data.endDate

      startDate.isBefore(LocalDate.now) &&
      startDate.isBefore(endDate) &&
      startDate.plusDays(data.duration).isEqual(endDate) &&
      data.duration >= 0 &&
      data.price >= 0
    }
  }
}
