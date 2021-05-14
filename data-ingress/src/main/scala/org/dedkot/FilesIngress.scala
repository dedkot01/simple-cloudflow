package org.dedkot

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{ Directory, DirectoryChangesSource }
import akka.stream.scaladsl.{ FileIO, Flow, Merge, RunnableGraph, Source }
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets.StreamletShape
import cloudflow.streamlets.avro._

import java.nio.charset.StandardCharsets
import java.nio.file.{ FileSystem, FileSystems, Path }
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.DurationInt

class FilesIngress extends AkkaStreamlet {

  val out: AvroOutlet[SubscriptionData] = AvroOutlet[SubscriptionData]("out")
  override val shape: StreamletShape    = StreamletShape.withOutlets(out)

  override def createLogic: RunnableGraphStreamletLogic = new RunnableGraphStreamletLogic() {

    override def runnableGraph: RunnableGraph[_] =
      mergedSource.via(readFile).via(parseRecord).to(plainSink(out))

    val fs: FileSystem = FileSystems.getDefault
    val path: Path     = fs.getPath(".\\data-ingress\\src\\main\\resources\\test-data")

    val directory: Source[Path, NotUsed] = Directory.ls(path)
    val directoryChangesCreation: Source[Path, NotUsed] = DirectoryChangesSource(path, 1.second, 1000).collect {
      case (path, DirectoryChange.Creation) => path
    }
    val mergedSource: Source[Path, NotUsed] = Source
      .combine(directory, directoryChangesCreation)(Merge[Path](_))
      .filter(isValidFile)

    val readFile: Flow[Path, Map[String, String], NotUsed] = Flow[Path].flatMapConcat { path =>
      FileIO
        .fromPath(path)
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
        .filter(map => isValidHeadersCSV(map.keys.toSet, path))
    }

    val parseRecord: Flow[Map[String, String], SubscriptionData, NotUsed] = Flow[Map[String, String]].map { record =>
      SubscriptionData(
        record("#").toLong,
        LocalDate.parse(record("StartDate")),
        LocalDate.parse(record("EndDate")),
        record("Duration").toLong,
        record("Price").toDouble
      )
    }

    def isValidFile(path: Path): Boolean = {
      // must be something ABC_1234_12345_01012021.csv
      val pattern  = raw"^\w{3}_\d{4}_\d{5}_\d{8}.csv$$".r
      val filename = path.getFileName.toString

      if (pattern.findFirstIn(filename).isEmpty) {
        system.log.warning(s"Filename is not valid: $filename")
        false
      }
      // check date file
      else {
        raw"\d{8}".r.findFirstIn(filename) match {
          case None =>
            system.log.warning(s"Filename is not valid: $filename")
            false
          case Some(dateStr) =>
            val format = DateTimeFormatter.ofPattern("ddMMyyyy")
            val date   = LocalDate.parse(dateStr, format)
            if (date.isAfter(LocalDate.now())) {
              system.log.warning(
                s"Date in filename ($date) is not valid, because today is ${LocalDate.now()}: $filename"
              )
              false
            } else true
        }
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
