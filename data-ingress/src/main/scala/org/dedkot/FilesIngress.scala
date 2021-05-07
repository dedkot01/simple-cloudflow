package org.dedkot

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.IOResult
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl._
import akka.util.ByteString
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._

import java.nio.charset.StandardCharsets
import java.nio.file._
import java.time.LocalDate
import scala.concurrent.Future
import scala.concurrent.duration._

class FilesIngress extends AkkaStreamlet {
  val out: AvroOutlet[SubscriptionData] = AvroOutlet[SubscriptionData]("out")
  override val shape: StreamletShape    = StreamletShape.withOutlets(out)

  override def createLogic: RunnableGraphStreamletLogic = new RunnableGraphStreamletLogic() {
    override def runnableGraph: RunnableGraph[Cancellable] =
      emitFromFilesContinuously.to(plainSink(out))

    val listFiles: NotUsed => Source[Path, NotUsed] = { _ =>
      Directory.ls(
        FileSystems.getDefault
          .getPath("C:\\Users\\dzhdanov\\IdeaProjects\\simple-cloudflow\\data-ingress\\src\\main\\resources")
      )
    }

    val readFile: Path => Source[Map[String, String], Future[IOResult]] = { path: Path =>
      FileIO.fromPath(path).via(CsvParsing.lineScanner()).via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
    }

    val parseFile: Map[String, String] => SubscriptionData = { csvRecord =>
      log.info(csvRecord.toString)
      SubscriptionData(
        csvRecord("#").toLong,
        LocalDate.parse(csvRecord("StartDate")),
        LocalDate.parse(csvRecord("EndDate")),
        csvRecord("Duration").toLong,
        csvRecord("Price").toDouble
      )
    }

    val emitFromFilesContinuously: Source[SubscriptionData, Cancellable] = Source
      .tick(1.second, 5.second, NotUsed)
      .flatMapConcat(listFiles)
      .flatMapConcat(readFile)
      .map(parseFile)
  }
}
