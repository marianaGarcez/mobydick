package test

import java.sql.Timestamp
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.GeometryFactory
import mobydick._

case class GeoLife(id: Int, position: Point, timestamp: Timestamp) {
  def this(line: String) {
    this(line.split(",")(0).toInt, new GeometryFactory().createPoint(MobyDick.latlonToUTM(line.split(",")(1).toDouble, line.split(",")(2).toDouble)), Timestamp.valueOf(line.split(",")(6) + " " + line.split(",")(7)))
  }
}

object GeoLife {
	def apply(line: String): GeoLife = {	
		var id = line.split(",")(0).toInt
		var point = new GeometryFactory().createPoint(MobyDick.latlonToUTM(line.split(",")(1).toDouble, line.split(",")(2).toDouble))
		var time = Timestamp.valueOf(line.split(",")(6) + " " + line.split(",")(7))
		var geoLife = new GeoLife(id, point, time)
		geoLife
	}
}