import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object Prosody extends App{
      override def main(args: Array[String]): Unit = { 

        // Command Line Args for setting directories
        val bronzeKey = args(0)
        val silverKey = args(1)

        // Build spark session
        val spark = SparkSession
                        .builder()
                        .appName("prosody")
                        .getOrCreate()

        // read 
        val textDF = spark.read
                        .option("wholetext", true)
                        .text(f"$bronzeKey")
                        .withColumnRenamed("value","text")
                        .withColumn("filename", input_file_name)
                        .coalesce(1)

        // textDF.write.mode("overwrite").parquet(f"$silverKey")

        // process text:
        // mark new lines with a " nnn " for which pincelate will return null
        // which will later be used as a delimiter during pattern-matching
        // replace tabs, hyphens, and \r are with space
        // replace non-alphabet characters, except space and apostrophe, with space
        // remove "'s"s from ends of words
        // replace multiple spaces with single spaces
        // explode
        val procTextDF = textDF
          .withColumn("procText1", lower(regexp_replace(col("text"), "[\\n]", " nnn "))).drop("text")
          .withColumn("procText2", lower(regexp_replace(col("procText1"), "[\\r\\t-]", " "))).drop("procText1")
          .withColumn("procText3", regexp_replace(col("procText2"), "[^a-zA-Z ']", " ")).drop("procText2")
          .withColumn("procText4", regexp_replace(col("procText3"), "'s\\b", "")).drop("procText3")
          .withColumn("procText5", regexp_replace(col("procText4"), "\\s+", " ")).drop("procText4")
          .withColumn("origWord", split(col("procText5"), " "))
          .select(col("filename"), explode(col("origWord")).as("origWord"))
        
        procTextDF.write.mode("overwrite").parquet(f"$silverKey")
    }
}
