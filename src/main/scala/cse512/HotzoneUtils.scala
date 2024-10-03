package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    //v3
    // Split coordinates
    val Array(x1Str, y1Str, x2Str, y2Str) = queryRectangle.split(",")
    val Array(xStr, yStr) = pointString.split(",")

    // Parse coordinates
    val x1 = x1Str.trim.toDouble
    val y1 = y1Str.trim.toDouble
    val x2 = x2Str.trim.toDouble
    val y2 = y2Str.trim.toDouble
    val x = xStr.trim.toDouble
    val y = yStr.trim.toDouble

    // engage coordinates and integrate them


    // Calculate boundaries
    val minX = x1 min x2
    val maxX = x1 max x2
    val minY = y1 min y2
    val maxY = y1 max y2

    // Check point in bounds
    if(x >= minX && x <= maxX && y >= minY && y <= maxY){
      return true
    }
      return false
  }
  }
