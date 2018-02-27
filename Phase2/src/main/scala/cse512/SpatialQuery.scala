package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String,pointString:String): Boolean ={
    val rect = queryRectangle.split(",").map(_.toDouble)
    val x_y = pointString.split(",").map(_.toDouble)
    if(rect(0) > x_y(0) | rect(2) < x_y(0) | rect(1) > x_y(1) | rect(3) < x_y(1)) {
      return false
    }
    return true
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean ={
    val p1 = pointString1.split(",").map(_.toDouble)
    val p2 = pointString2.split(",").map(_.toDouble)
    val dist = math.sqrt(math.pow((p1(0) - p2(0)),2) + math.pow((p1(1) - p2(1)), 2))
    if (dist <= distance) {
      return true
    }else {
      return false
    }
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    print("runRangeQuery begin and function register begin")
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    print(resultDf.count())
    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    print(resultDf.count())
    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1,pointString2,distance))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    print(resultDf.count())
    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1,pointString2,distance))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    print(resultDf.count())
    return resultDf.count()
  }
}
