package edu.vcu.sparksqlapi_example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer
import scala.util.Try

object MergeCrashData extends App {
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    System.setProperty("HADOOP_HOME", "C:/hadoop")

    val spark = SparkSession.builder()
        .appName("OD Causes Example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .getOrCreate()

    import spark.implicits._

    val odPath     = "data/vdh-overdose.csv"
    val acsPath    = "data/ACSDT1Y2024.B17018-Data.csv"
    val outputPath = "data/output"

    val odData = DataCleaner.cleanVDH(
        spark.read.format("csv")
            .option("header", "true")
            .option("sep", ",")
            .load(odPath),
        spark
    )

    val acsRaw = spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .load(acsPath)

    val acsFiltered = DataCleaner.cleanACS(acsRaw, spark)

    val acsData = acsFiltered.map(rw => acsSchema(
        geoId           = rw.getAs[String]("GEO_ID"),
        name            = rw.getAs[String]("NAME"),
        totalFamilies   = Try(rw.getAs[String]("B17018_001E").toInt).getOrElse(0),
        numBelowPoverty = Try(rw.getAs[String]("B17018_002E").toInt).getOrElse(0),
        fips            = rw.getAs[String]("GEO_ID").takeRight(5)
    ))

    val joined = acsData.join(
        odData,
        acsData("fips") === odData("Overdose ED Visit Patient FIPS"),
        "left"
    )

    val result = joined
        .na.drop(Seq("totalFamilies"))
        .na.drop(Seq("Overdose ED Visit Rate per 10,000 visits"))

    DataCleaner.deleteOutputIfExists(outputPath, spark)

    result.write.csv(outputPath)

    // ACS has one row per locality; after the join it is duplicated per OD row, so use first()
    val localityStats = result
        .groupBy("name", "totalFamilies", "numBelowPoverty")
        .agg(avg("Overdose ED Visit Rate per 10,000 visits").alias("avg_od_rate"))
        .withColumn("poverty_rate",
            round(col("numBelowPoverty") / col("totalFamilies") * 100, 2))

    val statsRow = localityStats.agg(
        min("poverty_rate").as("min_val"),
        max("poverty_rate").as("max_val")
    ).first()

    val minVal = statsRow.getAs[Double]("min_val")
    val maxVal = statsRow.getAs[Double]("max_val")
    val step   = (maxVal - minVal) / 4.0

    val splits = Array(
        Double.NegativeInfinity,
        minVal + step,
        minVal + 2 * step,
        minVal + 3 * step,
        Double.PositiveInfinity
    )

    val bucketizer = new Bucketizer()
        .setInputCol("poverty_rate")
        .setOutputCol("poverty_bucket_idx")
        .setSplits(splits)
        .setHandleInvalid("skip")

    val bucketed = bucketizer.transform(localityStats)

    val labelUDF = udf((idx: Double) => idx match {
        case 0.0 => "Low Poverty Rate"
        case 1.0 => "Medium-Low Poverty Rate"
        case 2.0 => "Medium-High Poverty Rate"
        case 3.0 => "High Poverty Rate"
        case _   => "Unknown"
    })

    // Map each locality name to its bucket label for joining back to row-level data
    val bucketLabels = bucketed
        .withColumn("poverty_bucket", labelUDF($"poverty_bucket_idx"))
        .select("name", "poverty_bucket", "poverty_rate")

    val joinedData = result
        .join(bucketLabels, Seq("name"), "left")
        .select(
            col("fips"),
            col("name"),
            col("Overdose ED Visit Count").cast("int").as("ed_visit_count"),
            col("Overdose ED Visit Rate per 10,000 visits").cast("double").as("ed_visit_rate"),
            col("totalFamilies").as("total_families"),
            col("numBelowPoverty").as("num_below_poverty"),
            col("poverty_rate"),
            col("poverty_bucket")
        )

    val detailedOutput = joinedData
        .groupBy("fips", "name", "poverty_bucket")
        .agg(
            sum("ed_visit_count").alias("ed_visit_count"),
            round(avg("ed_visit_rate"), 1).alias("avg_ed_visit_rate"),
            first("total_families").alias("total_families"),
            first("num_below_poverty").alias("num_below_poverty"),
            first("poverty_rate").alias("poverty_rate")
        )
        .orderBy("poverty_bucket", "fips")

    println("=== Aggregated by County: Poverty Bucket vs Overdose ED Visit Data ===")
    detailedOutput.show(200, truncate = false)

    val summary = bucketed
        .groupBy("poverty_bucket_idx")
        .agg(
            round(avg("avg_od_rate"), 2).alias("avg_overdose_rate"),
            round(avg("poverty_rate"), 2).alias("avg_poverty_rate")
        )
        .sort("poverty_bucket_idx")
        .withColumn("poverty_bucket", labelUDF($"poverty_bucket_idx"))
        .select("poverty_bucket", "avg_overdose_rate", "avg_poverty_rate")

    println("=== Poverty Bucket vs Avg Overdose ED Visit Rate ===")
    summary.show(truncate = false)
}
