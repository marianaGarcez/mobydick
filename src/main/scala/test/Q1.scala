package test

import mobydick._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.concurrent.TimeUnit
import com.vividsolutions.jts.geom._
import scala.collection.mutable.Buffer
import java.sql.Timestamp
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic


object Q1 {
  
  /*Q1
    * 
	* Find all cars within the area of interest (e.g. a rectangle)
	* 
	*/
  
  def main(args: Array[String]) {
    
    //random values
    val lat1 = 39.9916666666667
	val lon1 = 116.326533333333
	val lat2 = 40.0013130
	val lon2 = 116.9992424400
	
	var coordinate1 = MobyDick.latlonToUTM(lat1, lon1)
	var coordinate2 = MobyDick.latlonToUTM(lat1, lon2)
	var coordinate3 = MobyDick.latlonToUTM(lat2, lon1)
	var coordinate4 = MobyDick.latlonToUTM(lat2, lon2)
	
	var coords : Array[Coordinate] = Array(coordinate1, coordinate2, coordinate3, coordinate4, coordinate1)
	val factory = new GeometryFactory()
		
	var ring = factory.createLinearRing(coords)
	var holes : Array[LinearRing] = null
	var areaOfInterest = factory.createPolygon(ring, holes)
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)  
    val stream = env.socketTextStream("localhost", 9999)
    
    val geoLifeStream: DataStream[GeoLife] = stream.map{ tuple => GeoLife(tuple) }	
							
    val q1 = geoLifeStream
    	.assignAscendingTimestamps( tuple => tuple.timestamp.getTime ) 
    	.filter( geolife => geolife.position.within(areaOfInterest))
    	.map( geolife => (geolife.id, geolife.position))
    	
	q1.print
    
    env.execute("Apache Flink Geospatial")
  
  }
}