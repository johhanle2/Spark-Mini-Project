import org.apache.spark.sql.SparkSession

object CleanVDH extends App {

  val spark = SparkSession.builder()
    .appName("Clean VDH Overdose Data")
    .master("local[*]")
    .getOrCreate()

  val rawDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/vdh-overdose.csv")

  rawDf.createOrReplaceTempView("vdh_raw")

  val cleanDf = spark.sql("""
    SELECT
      `Overdose ED Visit Year`                    AS year,
      `Overdose ED Visit Patient FIPS`            AS fips,
      `Overdose ED Visit Patient Geography Name`  AS locality,
      `Overdose ED Visit Count`                   AS ed_visit_count,
      `Overdose ED Visit Rate per 10,000 visits`  AS ed_visit_rate
    FROM vdh_raw
    WHERE `Overdose ED Visit Patient Geography Level` = 'Locality'
      AND `Overdose ED Visit Drug Type`              = 'All Drug'
      AND `Overdose ED Visit Patient FIPS`           IS NOT NULL
      AND `Overdose ED Visit Count`                  IS NOT NULL
      AND `Overdose ED Visit Rate per 10,000 visits` IS NOT NULL
  """)

  cleanDf.createOrReplaceTempView("vdh_clean")
  cleanDf.show(10)
  println(s"Row count: ${cleanDf.count()}")

}
