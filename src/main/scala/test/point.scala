package test

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Coordinate

case class point(var point: Point)

object point {
	def apply(x: Double, y: Double): point = {
		val p: Point = new GeometryFactory().createPoint(new Coordinate(x, y))
        var pnt = new point(p)
        pnt
    }
}