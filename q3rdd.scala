import org.apache.spark.{SparkConf, SparkContext}

object q3 {

  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val conf = new SparkConf().setAppName("q3")
    val sc = new SparkContext(conf)

    // Read the flight dataset
    val flightData = sc.textFile("datasets/flights/flight.csv")

    // Filter the data to get relevant rows, probably not the best way for implementation
    val filteredData = flightData
      .filter(line => {
        val fields = line.split(",")
        fields(3) != fields(5) // Filter out rows where origin = dest
      })
    // Get itin. from filtered data.
    val tripDataRDD = filteredData
      .map(line => {
        val fields = line.split(",")
        (fields(0), fields(3), fields(5))
      })

    // Save the RDD as a text file
    tripDataRDD.saveAsTextFile("rddtrips") // Replace with the actual path

  }
}
