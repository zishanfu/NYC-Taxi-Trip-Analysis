package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    val recData = queryRectangle.split(",")
    val pointData = pointString.split(",")

    val x = (recData{0}.toDouble - pointData{0}.toDouble) * (recData{2}.toDouble - pointData{0}.toDouble)
    val y = (recData{1}.toDouble - pointData{1}.toDouble) * (recData{3}.toDouble - pointData{1}.toDouble)

    if (x <= 0 && y <= 0) {
      return true
    }
    else {
      return false
    }
  }
}
