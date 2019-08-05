import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val parsed = textFile.map(line => line.split(","))
      .map { case data =>
        val movie = data(0)
        val ratings = data.drop(1)
        (movie, ratings)
      }.cache()
    val pairs = parsed.collectAsMap()
    val broadcastVal = sc.broadcast(pairs)
    val output = parsed.flatMap { case (movie, ratings) => 
      val ratingsReference = broadcastVal.value
      ratingsReference.flatMap { case (k, v) =>
        if (k > movie){
          val similarity = v.zipAll(ratings, "", "").count {
            case (first, second) => first != "" && second != "" && first == second
          }
          Some(s"$movie,$k,${similarity.toString}")
        } else {
          None
        }
      }
    }
    output.saveAsTextFile(args(1))
  }
}
