import org.apache.spark.sql.SparkSession

object SparkRDDReference {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Simple RDD Example")
      .getOrCreate()

    // Create a SparkContext
    val sc = spark.sparkContext

    // Create an RDD using parallelize
    val numbers = Array(1, 2, 3, 4, 5)
    val numbersRDD = sc.parallelize(numbers)

    // Perform a map operation to square the numbers
    val squaredNumbersRDD = numbersRDD.map(number => number * number)

    // Collect and print the results
    val results = squaredNumbersRDD.collect()
    println("Squared numbers: " + results.mkString(", "))

    // Stop the SparkSession
    spark.stop()
  }
}
