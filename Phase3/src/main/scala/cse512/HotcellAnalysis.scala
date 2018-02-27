package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

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
    //pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    pickupInfo = pickupInfo.where(col("x") >= minX and col("x") <= maxX and col("y") >= minY and col("y") <= maxY and col("z") >= minZ and col("z") <= maxZ)
    pickupInfo = pickupInfo.groupBy("x", "y", "z").count()
    //pickupInfo.show()
    //pickupInfo.coalesce(1).write.mode(SaveMode.Overwrite).csv("test/out")

    val meanCount: Double = pickupInfo.agg(sum("count") / numCells).first.getDouble(0)
    val std: Double = math.sqrt(pickupInfo.agg(sum(pow("count", 2.0)) / numCells - math.pow(meanCount, 2.0)).first.getDouble(0))
    //println(meanCount)
    //println(std)
    pickupInfo.createOrReplaceTempView("hotcells")

    spark.udf.register("ST_Within", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => (HotcellUtils.ST_Within(x1, y1, z1, x2, y2, z2)))
    var joinDf = spark.sql("select a.x as x, a.y as y, a.z as z, b.count as count from hotcells a, hotcells b where ST_Within(a.x, a.y, a.z, b.x, b.y, b.z)")
    //joinDf.show()
    //joinDf.coalesce(1).write.mode(SaveMode.Overwrite).csv("test/out")
    joinDf = joinDf.groupBy("x", "y", "z").sum("count")
    newCoordinateName = Seq("x", "y", "z", "sum")
    joinDf = joinDf.toDF(newCoordinateName: _*)
    joinDf.createOrReplaceTempView("joindf")
    //joinDf.show()

    spark.udf.register("gScore", (sum: Int) => HotcellUtils.gScore(meanCount, std, numCells, sum))
    var scoreDf = spark.sql("select x, y, z, gScore(sum) as score from joindf")
    //scoreDf.sort(desc("score")).show()


    return scoreDf.sort(desc("score")).limit(50).select("x","y","z")

  }
}


