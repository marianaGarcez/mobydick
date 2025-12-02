package mobydick

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Coordinate


class Trajectory(var tDomain: TimePeriod, var lineS: LineString) {
    var _temporalDomain: TimePeriod = tDomain
    var _linestring: LineString = lineS
    
    def temporalDomain = {
      _temporalDomain
    }
    
    def temporalDomain_= (value: TimePeriod): Unit = {
		_temporalDomain = value
    }
    
    def linestring = {
      _linestring
    }
    
    def linestring_= (value: LineString): Unit = {
		_linestring = value
    }
    
    def this() {
      this(new TimePeriod(), new GeometryFactory().createLineString(Array(new Coordinate(1,1))))
    }

}
