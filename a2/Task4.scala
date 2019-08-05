import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val pairs = textFile.map(line => line.split(","))
      .map { case data =>
        val movie = data(0)
        val ratings = data.drop(1)
        
        (movie, ratings)
      }
      .collectAsMap()
    val broadcastVal = sc.broadcast(pairs)
    var result = List.empty[(String, String, String)]
    pairs.foreach { case (movie, ratings) => 
      val ratingsReference = broadcastVal.value
      ratingsReference.foreach { case (k, v) =>
        if (k > movie){
          val similarity = v.zipAll(ratings, "", "").count {
            case (first, second) => first != "" && second != "" && first == second
          }
          result = result :+ (movie, k, similarity.toString)
        }
      }
    }
    val output = result.map{ case (a,b,c) => s"$a,$b,$c" }
    sc.parallelize(output).saveAsTextFile(args(1))
  }
}
