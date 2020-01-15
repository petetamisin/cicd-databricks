package com.dbx.demos

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class PullCensusDataServiceTests extends FunSuite with BeforeAndAfter with MockitoSugar {

  test ("test Pull Census Data Service") {

    // (1) init
    val service = mock[PullCensusDataService]

    // (2) setup: when someone requests "John Mock", the service should work;
    //            when they try to pull in "James Mock", it should return not found.
    when(service.findPersonByName("John Mock")).thenReturn(Some(Person(1, "John", "Mock", 35, Some("M"))))
    when(service.findPersonByName("James Mock")).thenReturn(None)

    // (3) access the service
    val johnMock = service.findPersonByName("John Mock")
    val jamesMock = service.findPersonByName("James Mock")

    // (4) verify the results
    assert(johnMock == Some(Person(1, "John", "Mock", 35, Some("M"))))
    assert(jamesMock == None)
  }
}