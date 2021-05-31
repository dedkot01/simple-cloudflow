package org.dedkot

case class RecordStatus(
  fileData: FileData,
  maybeFail: Option[RecordFailStatus],
  maybeSuccess: Option[RecordSuccessStatus])
