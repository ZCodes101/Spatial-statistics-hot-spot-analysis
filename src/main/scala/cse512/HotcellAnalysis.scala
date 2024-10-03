package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    
    // v5
    // Process the pickup data to perform hot cell analysis
    val processedData = pickupInfo

    // Calculate the cell counts for each cell
    val cellCounts = processedData
      .filter(s"x >= $minX AND x <= $maxX AND y >= $minY AND y <= $maxY AND z >= $minZ AND z <= $maxZ")
      .groupBy("x", "y", "z")
      .count()
      .withColumnRenamed("count", "pointCount")
      .persist()

    // Compute summary statistics for the cell counts
    val summaryStats = cellCounts
      .selectExpr("SUM(pointCount) AS totalPoints", "SUM(pointCount * pointCount) AS totalSquares")
      .persist()

    val totalPoints = summaryStats.first().getLong(0).toDouble
    val totalSquares = summaryStats.first().getLong(1).toDouble

    val meanPoints = totalPoints / numCells
    val stndDevPoints = Math.sqrt((totalSquares / numCells) - (meanPoints * meanPoints))

    // Compute neigh stats for each cell
    val neighborStats = cellCounts
      .alias("base")
      .join(cellCounts.alias("neighbor"),
        expr("abs(base.x - neighbor.x) <= 1 AND abs(base.y - neighbor.y) <= 1 AND abs(base.z - neighbor.z) <= 1"))
      .groupBy("base.x", "base.y", "base.z")
      .agg(
        count("*").as("neighborCount"),
        sum("neighbor.pointCount").as("neighborSum")
      )
      .persist()

    // Register a UDF to calculate Z-Scores
    spark.udf.register("ZScoreCalc", (mean: Double, stddev: Double, neighbors: Int, sumPoints: Int, totalCells: Int) =>
      HotcellUtils.ZScoreCalc(mean, stddev, neighbors, sumPoints, totalCells)
      )

    // Calculate Z-Scores for each cell
    val zScores = neighborStats
      .selectExpr("x", "y", "z", s"ZScoreCalc($meanPoints, $stndDevPoints, neighborCount, neighborSum, $numCells) AS zscore")

    // Get top hot cells based on Z-Scores desc
    val topHotCells = zScores
      .orderBy(col("zscore").desc)
      .select("x", "y", "z")

     topHotCells

}
}
