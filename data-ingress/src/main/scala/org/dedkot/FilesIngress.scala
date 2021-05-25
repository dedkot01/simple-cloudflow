package org.dedkot

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{ Directory, DirectoryChangesSource }
import akka.stream.scaladsl.{ Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Source }
import akka.stream.{ ActorAttributes, ClosedShape, Supervision }
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro._

import java.nio.charset.StandardCharsets
import java.nio.file.{ FileSystem, FileSystems, Path }
import java.time.LocalDate
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import scala.concurrent.duration.DurationInt

class FilesIngress extends AkkaStreamlet {

  val dataOut: AvroOutlet[DataPacket] = AvroOutlet("data-out")
  //val statusSuccessOut: AvroOutlet[FileStatusSuccess] = AvroOutlet("file-status-success-out")
  val failStatusOut: AvroOutlet[FileFailStatus] = AvroOutlet("file-fail-status-out")

  override val shape: StreamletShape =
    StreamletShape.withOutlets(dataOut, failStatusOut)

  override def createLogic: RunnableGraphStreamletLogic = new RunnableGraphStreamletLogic() {

    override def runnableGraph: RunnableGraph[_] =
      RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val directory: Source[Path, NotUsed] = Directory.ls(path)
        val directoryChangesCreation: Source[Path, NotUsed] = DirectoryChangesSource(path, 1.second, 1000).collect {
          case (path, DirectoryChange.Creation) => path
        }
        val mergedSource: Source[Path, NotUsed] = Source
          .combine(directory, directoryChangesCreation)(Merge[Path](_))

        val validationFile: Flow[Path, Either[FileFailStatus, Path], NotUsed] = Flow[Path].map { path =>
          if (isValidFileName(path)) {
            if (isValidFileDate(path))
              Right(path)
            else
              Left(
                FileFailStatus(
                  FileData(path.getFileName.toString),
                  Seq(s"File ${path.getFileName} is not valid: date later than now")
                )
              )
          } else {
            Left(
              FileFailStatus(
                FileData(path.getFileName.toString),
                Seq(s"File ${path.getFileName} is not valid: filename doesn't correct")
              )
            )
          }
        }

        val broadcast = builder.add(Broadcast[Either[FileFailStatus, Path]](2))

        val readFile: Flow[Path, (Path, Map[String, String]), NotUsed] = Flow[Path].flatMapConcat { path =>
          FileIO
            .fromPath(path)
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
            .filter(map => isValidHeadersCSV(map.keys.toSet, path))
            .map(map => path -> map)
        }

        val sendStatusFail = plainSink(failStatusOut)

        val assemblyDataPacket: Flow[(Path, Map[String, String]), DataPacket, NotUsed] =
          Flow[(Path, Map[String, String])].map {
            case (path, record) =>
              DataPacket(
                FileData(path.getFileName.toString),
                SubscriptionData(
                  record("#").toLong,
                  LocalDate.parse(record("StartDate")),
                  LocalDate.parse(record("EndDate")),
                  record("Duration").toLong,
                  record("Price").toDouble
                )
              )
          }.withAttributes(
            ActorAttributes.supervisionStrategy {
              case e: DateTimeParseException =>
                system.log.warning(s""""${e.getParsedString}" could not be parsed in LocalDate, skip this record""")
                Supervision.Resume
              case _ => Supervision.Stop
            }
          )

        val sendData = plainSink(dataOut)

        mergedSource ~> validationFile ~> broadcast.in

        broadcast.out(0).filter(_.isLeft).map(_.left.get) ~> sendStatusFail
        broadcast.out(1).filter(_.isRight).map(_.right.get) ~> readFile ~> assemblyDataPacket ~> sendData

        ClosedShape
      })

    val fs: FileSystem = FileSystems.getDefault
    val path: Path     = fs.getPath(".\\data-ingress\\src\\main\\resources\\test-data")

    def isValidFileName(path: Path): Boolean = {
      // must be something ABC_1234_12345_01012021.csv
      val pattern  = raw"^\w{3}_\d{4}_\d{5}_\d{8}.csv$$".r
      val filename = path.getFileName.toString

      if (pattern.findFirstIn(filename).isEmpty) false
      else true
    }

    def isValidFileDate(path: Path): Boolean = {
      val filename = path.getFileName.toString

      raw"\d{8}".r.findFirstIn(filename) match {
        case None => false
        case Some(dateStr) =>
          val format = DateTimeFormatter.ofPattern("ddMMyyyy")
          val date   = LocalDate.parse(dateStr, format)

          if (date.isAfter(LocalDate.now())) false
          else true
      }
    }

    def isValidHeadersCSV(headers: Set[String], path: Path): Boolean = {
      val validHeaders = Set("#", "StartDate", "EndDate", "Duration", "Price")

      if (headers == validHeaders) true
      else {
        system.log.warning(s"""File "${path.getFileName}" having invalid record with headers: $headers""")
        false
      }
    }

  }
}
