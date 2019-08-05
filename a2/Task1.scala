import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val pairs = textFile.map(line => line.split(","))
      .map { case data =>
        val movie = data(0)
        val ratings = data.drop(1)
        
        (movie, ratings.map{i => 
          try {
            i.toInt
          } catch {
            case _: NumberFormatException => 0
          }
        })
      }
    val output = pairs.map { case (movie, ratings) => 
      val maxRating = ratings.max
      val highestRatings = ratings.zipWithIndex.filter(_._1.equals(maxRating)).map{ case (d, i) => (i+1).toString }.mkString(",")
      s"$movie,$highestRatings"
    }
    output.saveAsTextFile(args(1))
  }
}