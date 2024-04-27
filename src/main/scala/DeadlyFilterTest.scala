import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DeadlyFilterTest {

  // Number of value columns in the dataframe
  private val num_values = 167

  def main(args: Array[String]): Unit = {
val start= System.currentTimeMillis()
    // Configure Spark
    val spark = SparkSession.builder
      .appName("DeadlyFilterTest")
      .master("local[*]")
      .getOrCreate()

    // Create an schema with key and values
    val schema = StructType(
      StructField("key", StringType, nullable = false) +:
        (1 to num_values).map(i =>
          StructField(s"value_$i", StringType, nullable = false)
        )
    )

    // Create an empty DataFrame with the specified schema
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // Filter all the rows that only have empty values
    val filter = df.columns
      .diff(Seq("key"))
      .map(col)
      .map(_ =!= lit("")).reduce(_ || _)
    val dfFilter = df.filter(filter)

    // Rename all the columns
    val dfRename = df.columns
      .foldLeft(dfFilter){ (df, colName) =>
        df.withColumnRenamed(colName, s"ren_$colName")
      }

    // Join the renamed+filtered dataframe with the original one
    val dfJoin = dfRename.join(df, dfRename("ren_key") === df("key"))

    // Show the result of the join
    dfJoin.show()
val end = System.currentTimeMillis()
    scala.io.StdIn.readLine(s"${end-start}")
    // Stop Spark
    spark.stop()
  }
}