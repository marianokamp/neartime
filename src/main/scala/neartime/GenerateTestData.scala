package neartime

object GenerateTestData {

  def main(args: Array[String]): Unit = {
    createTestData(500)
  }

  def createTestData(N: Int = 500) =  {

    var nextId = -1

    val testData = for (i <- List.range(0, N)) yield {
      val predecessorId: Int = if (scala.math.random < 0.8) nextId else -1;
      nextId += 1
      Event(nextId, predecessorId, "")
    }
    scala.util.Random.shuffle(testData)
  }

  case class Event(id: Int, predecessorId: Int, text: String)
}
