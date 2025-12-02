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


object Q4 {

   /*Q4
    * 
	* Find all cars/drivers that have travelled more than 10 km during last hour.
	* 
	*/
 
  def main(args: Array[String]) {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val stream = env.socketTextStream("localhost", 9999)
    
    val geoLifeStream: DataStream[GeoLife] = stream.map{ tuple => GeoLife(tuple) }
    
  	val q4 = geoLifeStream
		.assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
		.keyBy(0)
		.timeWindow(Time.of(60, TimeUnit.MINUTES)) 
		.apply { MobyDick.mobilePoint _ }
  		.filter( mo => mo.location.lengthAtTime(mo.location.endTime) > 10000)
  		.map( mo => (mo.id, mo.location.lengthAtTime(mo.location.endTime)) )

    q4.print 
    
    env.execute("Apache Flink Geospatial")
  
  }
}