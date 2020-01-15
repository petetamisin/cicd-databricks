package com.dbx.demos

// a very simple Person class
case class Person(
  id: Int,
  firstName: String,
  lastName: String,
  age: Int,
  gender: Option[String])

trait PullCensusDataService {
  def findPersonByName(fullName: String): Option[Person]
}

// the code for our real/live PullCensusDataService
class PullSeededCensusDataService extends PullCensusDataService {
  // implementation here ...
  private val persons = Map("John Doe" -> Person(1, "John", "Doe", 32, Some("male")),
                          "Johanna Doe" -> Person(2, "Johanna", "Doe", 30, None))
  def findPersonByName(fullname: String): Option[Person] = persons.get(fullname)
  def findAll = persons.values
}
