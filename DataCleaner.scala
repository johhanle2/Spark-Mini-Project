package edu.vcu.sparkminiproject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}

object DataCleaner {

    def cleanVDH(df: DataFrame, spark: SparkSession): DataFrame = {
        df.na.drop(Seq(
            "Overdose ED Visit Patient FIPS",
            "Overdose ED Visit Count",
            "Overdose ED Visit Rate per 10,000 visits"
        ))
    }

    def cleanACS(df: DataFrame, spark: SparkSession): DataFrame = {
        df.filter(row => {
            val geoId           = row.getAs[String]("GEO_ID")
            val povertyVal      = row.getAs[String]("B17018_001E")
            val belowPovertyVal = row.getAs[String]("B17018_002E")
            geoId != null && geoId.nonEmpty &&
            povertyVal != null && povertyVal.nonEmpty &&
            povertyVal != "null" && !povertyVal.startsWith("Estimate") &&
            belowPovertyVal != null && belowPovertyVal.nonEmpty &&
            belowPovertyVal != "null"
        })
    }

    def deleteOutputIfExists(outputPath: String, spark: SparkSession): Unit = {
        val fs   = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val path = new Path(outputPath)
        if (fs.exists(path)) fs.delete(path, true)
    }
}
