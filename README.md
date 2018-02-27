# NYC-Taxi-Trip-Analysis
A in-memory applications using Spark and Spark SQL.

## Phase2
Implemented User Defined Functions and finished four spatial queries:
  * Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.
  * Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs such that the point is within the rectangle.
  * Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D from P
  * Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1, s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).

## Phase3
Implemented spatial hot spot analysis and finished two different hot spot analysis tasks:
  * Hot zone analysis: This task will needs to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it include more points. So this task is to calculate the hotness of all the rectangles.
  * Hot cell analysis: This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.
  The Problem Definition page is here: http://sigspatial2016.sigspatial.org/giscup2016/problem
  
  
