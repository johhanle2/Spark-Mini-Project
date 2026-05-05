import org.apache.spark.sql.SparkSession

object AnalyzeData extends App {

  val spark = SparkSession.builder()
    .appName("Overdose Poverty Analysis")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("data/vdh-overdose.csv")
    .createOrReplaceTempView("vdh_raw")

  val vdhClean = spark.sql("""
    SELECT
      `Overdose ED Visit Year`                    AS year,
      `Overdose ED Visit Patient FIPS`            AS fips,
      `Overdose ED Visit Patient Geography Name`  AS locality,
      `Overdose ED Visit Count`                   AS ed_visit_count,
      `Overdose ED Visit Rate per 10,000 visits`  AS ed_visit_rate
    FROM vdh_raw
    WHERE `Overdose ED Visit Patient Geography Level` = 'Locality'
      AND `Overdose ED Visit Drug Type`             = 'All Drug'
      AND `Overdose ED Visit Patient FIPS`          IS NOT NULL
  """)
  vdhClean.createOrReplaceTempView("vdh_clean")

  val acs = spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("data/ACSDT1Y2024.B17018-Data.csv")

  acs
    .filter(acs("GEO_ID") =!= "Geography")
    .createOrReplaceTempView("acs_raw")

  val joined = spark.sql("""
    SELECT
      v.year,
      v.fips,
      v.locality,
      v.ed_visit_count,
      v.ed_visit_rate,
      a.total_families,
      a.below_poverty
    FROM vdh_clean v
    INNER JOIN (
      SELECT
        SUBSTRING(GEO_ID, -5, 5) AS fips,
        NAME AS county_name,
        B17018_001E AS total_families,
        B17018_002E AS below_poverty
      FROM acs_raw
      WHERE NAME LIKE '%Virginia%'
        AND B17018_001E IS NOT NULL
        AND B17018_001E != 'null'
        AND B17018_002E IS NOT NULL
        AND B17018_002E != 'null'
    ) a ON CAST(v.fips AS STRING) = CAST(a.fips AS STRING)
    WHERE a.total_families IS NOT NULL
      AND a.total_families != 'null'
      AND a.below_poverty IS NOT NULL
      AND a.below_poverty != 'null'
  """)
  joined.createOrReplaceTempView("joined_data")

  val withPovertyRate = spark.sql("""
    SELECT
      *,
      ROUND((CAST(below_poverty AS DOUBLE) / CAST(total_families AS DOUBLE)) * 100, 2) AS poverty_rate
    FROM joined_data
  """)
  withPovertyRate.createOrReplaceTempView("with_poverty_rate")

  val bucketed = spark.sql("""
    SELECT
      *,
      CASE
        WHEN poverty_rate < 5  THEN 'Low (under 5%)'
        WHEN poverty_rate < 10 THEN 'Medium (5-10%)'
        ELSE                        'High (10%+)'
      END AS poverty_bucket
    FROM with_poverty_rate
  """)
  bucketed.createOrReplaceTempView("bucketed_data")

  val result = spark.sql("""
    SELECT
      poverty_bucket,
      COUNT(*) AS num_records,
      ROUND(AVG(CAST(ed_visit_rate AS DOUBLE)), 2) AS avg_ed_visit_rate,
      ROUND(AVG(poverty_rate), 2) AS avg_poverty_rate
    FROM bucketed_data
    GROUP BY poverty_bucket
    ORDER BY avg_ed_visit_rate DESC
  """)

  println("Average overdose ED visit rate by poverty bucket (localities grouped by % of families below poverty line):")
  result.show()

}
