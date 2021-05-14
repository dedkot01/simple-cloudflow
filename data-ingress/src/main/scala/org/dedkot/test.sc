import java.time.LocalDate
import java.time.format.DateTimeFormatter

val format = DateTimeFormatter.ofPattern("ddMMyyyy")
val date = "32042021"

val localDate = LocalDate.parse(date, format)
