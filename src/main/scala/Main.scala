import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Prosody extends App{

      override def main(args: Array[String]): Unit = { 

        // Command Line Args for setting directories
        val bronzeKey = args(0)
        val silverKey = args(1)
        val goldKey = args(2)
        val stressDictLocation = args(3)
        val soundoutScript = args(4)
        // val stressDictBkLocation = args(5)

        // Build spark session
        val spark = SparkSession
                        .builder()
                        // .config("spark.files", soundoutScript)
                        .appName("prosody")
                        .getOrCreate()

        // read 
        val textDF = spark.read
                        .option("wholetext", true)
                        .text(f"$bronzeKey")
                        .withColumnRenamed("value","text")
                        .withColumn("filename", input_file_name)
                        .coalesce(1)

        // clean text:
        // mark new lines with a " nnn " for which pincelate will return null
        // which will later be used as a delimiter during pattern-matching
        // replace tabs, hyphens, and \r are with space
        // replace non-alphabet characters, except space and apostrophe, with space
        // remove "'s"s from ends of words
        // replace multiple spaces with single spaces
        // explode
        val cleanTextDF = textDF
          .withColumn("cleanText1", lower(regexp_replace(col("text"), "[\\n]", " nnn "))).drop("text")
          .withColumn("cleanText2", lower(regexp_replace(col("cleanText1"), "[\\r\\t-]", " "))).drop("cleanText1")
          .withColumn("cleanText3", regexp_replace(col("cleanText2"), "[^a-zA-Z ']", " ")).drop("cleanText2")
          .withColumn("cleanText4", regexp_replace(col("cleanText3"), "'s\\b", "")).drop("cleanText3")
          .withColumn("cleanText", regexp_replace(col("cleanText4"), "\\s+", " ")).drop("cleanText4")
        
        // store clean text as silver copy
        cleanTextDF.write.mode("overwrite").parquet(silverKey)
        
        // read stress  dictionary
        val stressDictSchema = StructType(Array(
          StructField("dictWord", StringType, nullable = true),
          StructField("stress", StringType, nullable = true)
        ))
        val stressDict = spark.read
          .schema(stressDictSchema)
          .parquet(stressDictLocation)

        // find phonemes for each word
        // by joining with stress dict
        // try with and without leading and ending apostrophes
        // coalesce the found phonemes into single column
        // val cleanTextDF = spark.read.parquet(file_location)
        val textStressDF = cleanTextDF
          .withColumn("origWord", split(col("cleanText"), " "))
          .select(col("filename"), explode(col("origWord")).as("origWord"))
          .join(broadcast(stressDict), col("origWord") === col("dictWord"), "left")
          .withColumnRenamed("stress", "origStress").drop("pronunciation").drop("dictWord")
          .withColumn("origWordNoApos", regexp_replace(col("origWord"), "\\b'$|^'\\b", ""))
          .join(broadcast(stressDict), col("origWordNoApos") === col("dictWord"), "left")
          .withColumnRenamed("stress", "origNoAposStress").drop("pronunciation")
          .withColumn("stress", coalesce(col("origStress"), col("origNoAposStress")))

        // find words not found in stress dict
        val unknownWordsDF = textStressDF
          .filter(col("stress").isNull && col("origWordNoApos").isNotNull && trim(col("origWordNoApos")) != "")
          .select(col("filename"), col("origWordNoApos"))
          .distinct
          .groupBy("filename")
          .agg(collect_set("origWordNoApos").alias("unknownWords"))
      //    .withColumn("concatWords", mkString(col("words")))
          .select(col("unknownWords"))
          .coalesce(1)
        
        // unknownWordsDF.write.mode("overwrite").parquet(goldKey)
        
        // get stress from pincelate
        val unknownWordsRDD = unknownWordsDF.rdd.repartition(1)
        val pipeRDD = unknownWordsRDD.pipe(soundoutScript)
        
        // make df from pincelate output
        import spark.implicits._
        val stressDF = pipeRDD.toDF("stress")
          .filter(col("stress").isNotNull && trim(col("stress")) =!= "")
          .coalesce(1)
          .withColumn("stressSplit", split(col("stress"),","))
          .select(col("stressSplit").getItem(0).as("dictWord"), col("stressSplit").getItem(1).as("stress"))
    
        // save new word-stress pairs
        // stressDF.write.mode("overwrite").parquet(stressOutput)

        // combine new word-stress pairs with old ones
        val newStressDict = stressDict.union(stressDF)

        // update stressDict
        newStressDict.write.mode("overwrite").parquet(stressDictLocation)

        // apply new stress patterns
        // concat words -> text, stresses -> stress sequence
        // num rows in final df == num files uploaded
        val joinStressDict = newStressDict.withColumnRenamed("stress","newStress")
        val finalTextStressDF = textStressDF.withColumnRenamed("stress","existingStress")
          .select(col("filename"), col("origWordNoApos"), col("existingStress"))
          .join(broadcast(joinStressDict), col("origWordNoApos") === col("dictWord"), "left")
          .withColumn("stressRaw", coalesce(col("existingStress"),col("newStress")))
          .select(col("filename"), col("origWordNoApos"), col("stressRaw"))
          .withColumn("stress", when(col("origWordNoApos") === "nnn", 9)
            .when(col("stressRaw").isNull, 9)
            .otherwise(col("stressRaw")))
          .select(col("filename"), col("origWordNoApos"), col("stress"))
          .filter(trim(col("origWordNoApos")) =!= "")
          .groupBy("filename")
          .agg(
            concat_ws(" ", collect_list("origWordNoApos")).alias("text"),
            concat_ws("", collect_list("stress")).alias("stress")
          )

        // store text and stress sequence as gold copy
        finalTextStressDF.write.mode("overwrite").parquet(goldKey)
    }
}
