import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(","))
      .flatMap { case data =>
        data.zipWithIndex.drop(1)
      }.map { case (data, i) =>
        if (data.equals("")){
          (i, 0)
        } else {
          (i, 1)
        }
      }.reduceByKey(_ + _).map { case (i, count) =>
        s"$i,${count.toString}"
      }
    output.saveAsTextFile(args(1))
  }
}
