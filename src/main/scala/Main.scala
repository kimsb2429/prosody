import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object Prosody extends App{
      override def main(args: Array[String]): Unit = { 

        // Command Line Args for setting directories
        val inputKey = args(0)
        val outputKey = args(1)

        val spark = SparkSession
                        .builder()
                        .appName("prosody")
                        .getOrCreate()

        val texts = spark.read
                        .option("wholetext", true)
                        .text(f"$inputKey")
                        .withColumnRenamed("value","text")
                        .withColumn("filename", input_file_name)
                        .coalesce(1)

        texts.write.mode("overwrite").parquet(f"$outputKey")

    }
}
