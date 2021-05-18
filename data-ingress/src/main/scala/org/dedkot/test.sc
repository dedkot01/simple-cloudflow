import java.time.LocalDate
import java.time.format.DateTimeFormatter

val format = DateTimeFormatter.ofPattern("ddMMyyyy")
val date = "30042021"

val localDate = LocalDate.parse(date, format)

val list1 = List("#", "StartDate")
val list2 = List("#", "StartDate")
val list3 = List("StartDate", "#")

list1 == list2
list1 == list3

val set1 = Set("#", "StartDate")
val set2 = Set("#", "StartDate")
val set3 = Set("StartDate", "#")

set1 == set2
set1 == set3

val map = Map("key1" -> "val1", "key1" -> "val2")
map("key1")
