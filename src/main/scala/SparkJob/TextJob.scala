package SparkJob

import SparkJob.Domain._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.annotation.tailrec

object TextJob extends DataJob[DataFrame, DataFrame] {

    case class Stress(filename: String, stress: String)

    override def read(params:SparkParams)(implicit spark: SparkSession) = {
        var dataReader = spark.read
        params.inOptions.toSeq.foreach{
            op => dataReader = dataReader.option(op._1, op._2)
        }
        val inputDF = dataReader.text(params.inPath)
        
        inputDF
    }

    override def transform(textDF: DataFrame)(implicit spark:SparkSession, sparkParams:SparkParams) = {
        
        // clean text:
        // - mark new lines with a " nnn " for which the dictionary will return 9
        // which is a delimiter used during pattern-matching
        // - replace tabs, hyphens, and \r with space
        // - replace non-alphabet characters, except space and apostrophe, with space
        // - remove apostrophe-s's from ends of words
        // - replace multiple spaces with single spaces
        // - explode to get one word per row
        val cleanTextDF = textDF
          .withColumn("cleanText1", lower(regexp_replace(col("text"), "[\\n]", " nnn "))).drop("text")
          .withColumn("cleanText2", lower(regexp_replace(col("cleanText1"), "[\\r\\t-]", " "))).drop("cleanText1")
          .withColumn("cleanText3", regexp_replace(col("cleanText2"), "[^a-zA-Z ']", " ")).drop("cleanText2")
          .withColumn("cleanText4", regexp_replace(col("cleanText3"), "'s\\b", "")).drop("cleanText3")
          .withColumn("cleanText", regexp_replace(col("cleanText4"), "\\s+", " ")).drop("cleanText4")
        
        // read stress  dictionary
        val stressDictSchema = StructType(Array(
          StructField("dictWord", StringType, nullable = true),
          StructField("stress", StringType, nullable = true)
        ))
        val stressDict = spark.read
          .schema(stressDictSchema)
          .parquet(sparkParams.refPath)

        // find phonemes for each word by joining with stress dict
        // try with and without leading and ending apostrophes
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
          .select(col("unknownWords"))
          .filter(size(col("unknownWords")) > 1)
          .coalesce(1)
        
        // convert to rdd with 1 partition
        val unknownWordsRDD = unknownWordsDF.rdd.repartition(1)
        var newStressDict = stressDict

        // if there are unknown words
        if(!unknownWordsRDD.isEmpty) {

          // get stress from pincelate
          val pipeRDD = unknownWordsRDD.pipe(sparkParams.pipePath)
          
          // make df from pincelate output
          import spark.implicits._
          val stressDF = pipeRDD.toDF("stress")
            .filter(col("stress").isNotNull && trim(col("stress")) =!= "")
            .coalesce(1)
            .withColumn("stressSplit", split(col("stress"),","))
            .select(col("stressSplit").getItem(0).as("dictWord"), col("stressSplit").getItem(1).as("stress"))

          // update stressDict
          stressDF.write.mode("append").parquet(sparkParams.refPath)
          newStressDict = stressDict.union(stressDF)

        }
        
        // apply new stress patterns
        // groupby to get one fulltext and one stress sequence per row
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

        // store text and stress sequence as silver copy
        finalTextStressDF.write.mode("append").parquet(sparkParams.intermediaryResultPath)

        // read stress pattern dataframe as dataset
        import spark.implicits._
        val stressDS = finalTextStressDF.select(col("filename"),col("stress"))
          .as[Stress]

        // counts and normalizes pattern matches
        def countMatch(s: String, pattern: String): Double = {
          val reg = pattern.r
          return 1.0 * reg.findAllIn(s).length / s.length
        }

        // spark udf for countMatch
        val countMatchUDF = udf((s: String, pattern: String) => countMatch(s, pattern))

        // build dataframe of all profiling patterns and their normalized counts
        @tailrec
        def profile(df: Dataset[Stress], patternList: List[String]): Dataset[Stress] = {
          def profilePattern(df: Dataset[Stress], pattern: String): Dataset[Stress] = {
            df.withColumn(pattern, countMatchUDF(col("stress"), lit(pattern))).as[Stress]
          }
          if (patternList.length == 1) profilePattern(df, patternList.last)
          else profile(profilePattern(df, patternList.last), patternList.take(patternList.length-1))
        }

        // analyze proportion of iamb, dactyl, anapest, trochee, and spondee
        // relative to the length of the text
        val patternsToAnalyze = List("01", "10", "001", "100", "11")
        val profileDF = profile(stressDS, patternsToAnalyze)
          .withColumn("filenameSplit", split(col("filename"),"/"))
          .withColumn("titleWithFileExt", element_at(col("filenameSplit"), -1))
          .withColumn("title",substring_index(col("titleWithFileExt"), ".",1))
          .drop(col("filename"))
          .drop(col("stress"))
          .drop(col("filenameSplit"))
          .drop(col("titleWithFileExt"))

        // save a gold copy of the analysis
        SaveParameters(profileDF,sparkParams)

    }

    override def save(p:SaveParameters) = {
        p.df.write
        .partitionBy(p.params.partitionColumn)
        .options(p.params.outOptions)
        .format(p.params.outFormat)
        .mode(p.params.saveMode)
        .save(p.params.outPath)
    }

}