import org.apache.spark.sql.SparkSession

object CleanACS extends App {

  val spark = SparkSession.builder()
    .appName("Clean ACS Poverty Data")
    .master("local[*]")
    .getOrCreate()

  val rawDf = spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("data/ACSDT1Y2024.B17018-Data.csv")

  val filteredDf = rawDf.filter("GEO_ID != 'Geography'")

  filteredDf.createOrReplaceTempView("acs_raw")

  val cleanDf = spark.sql("""
    SELECT
      SUBSTRING(GEO_ID, -5, 5)  AS fips,
      NAME                       AS county_name,
      B17018_001E                AS total_families,
      B17018_002E                AS below_poverty
    FROM acs_raw
    WHERE NAME LIKE '%Virginia%'
      AND B17018_001E IS NOT NULL AND B17018_001E != 'null'
      AND B17018_002E IS NOT NULL AND B17018_002E != 'null'
  """)
}
