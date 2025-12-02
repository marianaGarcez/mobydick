package test

import com.vividsolutions.jts.io.WKTReader
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


object Q2 {

   
   /*Q2
    * 
	* For each car, find its minimal distance from the point of interest during last half an hour.
	* 
	* POINT(556940 6319971)
	*/
  
  def main(args: Array[String]) {
    
    val factory = new GeometryFactory()
    val pointOfInterest: Point = factory.createPoint(new Coordinate(442414.25, 4426973.53))
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val stream = env.socketTextStream("localhost", 9999)
    
    val geoLifeStream: DataStream[GeoLife] = stream.map{ tuple => GeoLife(tuple) }

    val q2 = geoLifeStream
	  .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(30, TimeUnit.MINUTES)) // 30 minutes - for testing 1
      .apply { MobyDick.mobilePoint _ }
      .map( mo => (mo.id, mo.location.minDistance(pointOfInterest)) )

    q2.print
    
    env.execute("Apache Flink Geospatial")
  
  }
}