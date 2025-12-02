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

object Q6 {
   
  /*Q6
	* Find trajectories of the cars that were within 500 meters from a point
	* of interest within last 30 minutes.
	* 
	* POINT(425868.18 4435627.75)
	*/
     
  def main(args: Array[String]) {
    
    val factory = new GeometryFactory()
    val pointOfInterest: Point = factory.createPoint(new Coordinate(441144.4, 4431129.8))
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val stream = env.socketTextStream("localhost", 9999)
    
    val geoLifeStream: DataStream[GeoLife] = stream.map{ tuple => GeoLife(tuple) }
    
   	val q6 = geoLifeStream
   	  .assignAscendingTimestamps( tuple => tuple.timestamp.getTime ) 
      .keyBy(0)
      .timeWindow(Time.of(60, TimeUnit.MINUTES)) 
      .apply { MobyDick.mobilePoint _ }
      .filter( mo => mo.location.distance(pointOfInterest, mo.location.endTime).asInstanceOf[Double] < 500 )
      .map( mo => (mo.id, mo.location.subTrajectory(mo.location.startTime, mo.location.endTime)))

    q6.print
    
    env.execute("Apache Flink Geospation")
  
  }
}