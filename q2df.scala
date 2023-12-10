import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst._

object df{
	def main(args:Array[String])
		{
		val spark = SparkSession.builder().getOrCreate()
		val sch = new StructType()
		.add("ITIN_ID", StringType, true)
		.add("YEAR", StringType, true)
		.add("QUARTER", IntegerType, true)
		.add("ORIGIN", StringType, true)
		.add("DEST", StringType, true)
		.add("DEST_STATE_NM", StringType, true)
		.add("PASSENGERS", LongType, true)
		val frame = spark.read.format("csv").schema(sch).load("/datasets/flight/flight.csv").where(col("ITIN_ID) =!= "ITIN_ID)	
		// Get distinct itineraries
   		 val distinctItin = frame
      			.select("ITIN_ID", "ORIGIN")
      			.distinct()
		val tripData = distinctItin.alias("a")
      			.join(frame.alias("b"), col("a.ITIN_ID") === col("b.ITIN_ID"))
      			.where(col("a.ORIGIN") =!= col("b.DEST"))
      			.select("a.ITIN_ID", "a.ORIGIN", "b.DEST")
      			.distinct()
		tripData.save.text("dftrips")
	}
}
