import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.{NullWritable}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(","))
      .map(line => 
        line.drop(1)
        .filter(!_.equals(""))
        .size).coalesce(1)
      .reduce(_ + _)
    
    sc.parallelize(Seq(output), 1).saveAsTextFile(args(1))
  }
}