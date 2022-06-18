import org.apache.log4j._
import org.apache.spark.sql.SparkSession
// import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.sys.process._

object Prosody extends App{

      override def main(args: Array[String]): Unit = { 

        // Command Line Args for setting directories
        val bronzeKey = args(0)
        // val bronzeKey = "s3://prosodies-bronze/20220613162722/"
        val silverKey = args(1)
        // val cmuDictLocation ="s3://prosodies/find-phonemes-input-path/cmudict.parquet/"
        val goldKey = args(2)
        val cmuDictLocation = args(3)
        // val soundoutScriptPath = "s3://prosodies/soundout.py"
        val soundoutScript = args(4)
        val stressOutput = args(5)

        // Build spark session
        val spark = SparkSession
                        .builder()
                        .config("spark.files", soundoutScript)
                        .appName("prosody")
                        .getOrCreate()

        // Get spark context
        // val sc = spark.sparkContext
        // sc.addFile(soundoutScript)
        // val soundoutScriptPath = "/home/ec2-user/" + soundoutScript.split("/").last
        // sc.addFile(soundoutScriptPath) 

        // read 
        val textDF = spark.read
                        .option("wholetext", true)
                        .text(f"$bronzeKey")
                        .withColumnRenamed("value","text")
                        .withColumn("filename", input_file_name)
                        .coalesce(1)

        // textDF.write.mode("overwrite").parquet(f"$silverKey")

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
        cleanTextDF.write.mode("overwrite").parquet(f"$silverKey")
        


        // val file_location = "/FileStore/tables/part_00000_9a6e7a58_7ac5_4485_839a_2fd3b7cc8b41_c000_snappy-3.parquet"
        // val cmuDictLocation = "/FileStore/tables/part_00000_fabfd926_9318_4902_ad7b_e0e0f707f28d_c000_snappy.parquet"
        // val file_location = "/Users/jaekim/Downloads/part-00000-9a6e7a58-7ac5-4485-839a-2fd3b7cc8b41-c000.snappy.parquet"
        // val cmuDictLocation ="s3://prosodies/find-phonemes-input-path/cmudict.parquet/"
        // read cmu phoneme dictionary
        val cmuDictSchema = StructType(Array(
          StructField("dictWord", StringType, nullable = true),
          StructField("pronunciation", StringType, nullable = true),
          StructField("stress", StringType, nullable = true)
        ))
        val cmuDict = spark.read
          .schema(cmuDictSchema)
          .parquet(cmuDictLocation)
          .cache
        // find phonemes for each word
        // by joining with cmu dict
        // try with and without leading and ending apostrophes
        // coalesce the found phonemes into single column
        // val cleanTextDF = spark.read.parquet(file_location)
        def getStress(word: String): String = {
            "hello"
        }
        val getStressUDF = udf[String, String](getStress).asNondeterministic()

        val textStressDF = cleanTextDF
          .withColumn("origWord", split(col("cleanText"), " "))
          .select(col("filename"), explode(col("origWord")).as("origWord"))
          .join(broadcast(cmuDict), col("origWord") === col("dictWord"), "left")
          .withColumnRenamed("stress", "origStress").drop("pronunciation").drop("dictWord")
          .withColumn("origWordNoApos", regexp_replace(col("origWord"), "\\b'$|^'\\b", ""))
          .join(broadcast(cmuDict), col("origWordNoApos") === col("dictWord"), "left")
          .withColumnRenamed("stress", "origNoAposStress").drop("pronunciation")
          .withColumn("stress", coalesce(col("origStress"), col("origNoAposStress")))
          .filter(col("origWord").isNotNull)
          .withColumn("stress",getStressUDF(col("origWord")))
        // val df = spark.read.parquet(file_location)


        // find the words that were not in cmu dict
        val unknownWordsDF = textStressDF
          .filter(col("stress").isNull && col("origWordNoApos").isNotNull && trim(col("origWordNoApos")) != "")
          .select(col("filename"), col("origWordNoApos"))
          .distinct
          .groupBy("filename")
          .agg(collect_set("origWordNoApos").alias("unknownWords"))
      //    .withColumn("concatWords", mkString(col("words")))
          .select(col("unknownWords"))
          .coalesce(1)

        
        val soundoutScriptName = soundoutScript.split("/").last
        // val soundoutScriptPath = "./" + soundoutScriptName
        val soundoutScriptPath = "/home/ec2-user/" + soundoutScriptName
        // val soundoutScriptPath = SparkFiles.get(soundoutScriptName)
        // val cmd = Seq("chmod", "777", soundoutScriptPath)
        // cmd !
        val unknownWordsRDD = unknownWordsDF.rdd
        //  // dbutils.fs.cp("dbfs:/FileStore/tables/test-1.py", "file:///tmp/test.py")
      //  // dbutils.fs.ls("file:/tmp/test.py")
      //  // val soundoutScriptPath = "file:/tmp/test.py"
        // val soundoutScriptPath = "s3://prosodies/soundout.py"
        // val soundoutScriptPath = "/Users/jaekim/wcd/wcd/hello_world/test.py"
        // val pipeRDD = unknownWordsRDD.pipe(Seq(SparkFiles.get(soundoutScriptName)))
        // val pipeRDD = unknownWordsRDD.pipe(SparkFiles.get(soundoutScriptPath))
        val pipeRDD = unknownWordsRDD.pipe(soundoutScriptPath)

        // println(pipeRDD.count)
      //  pipeRDD.foreach(println)

        import spark.implicits._
        pipeRDD.toDF("stress")
          .coalesce(1)
          .write
          .mode("overwrite")
          .save(stressOutput)
          // .save("s3://prosodies/stress.parquet")
        

    }
}
