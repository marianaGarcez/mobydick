package test


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
import mobydick._



object Q0 {

  /*Q0
    * 
    * Continuously each minute, show location of drivers who travelled 
    * more than 3 km in past 10 minutes
    *  
	*/
 
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment   
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 9999)
    
    val geoLifeStream: DataStream[GeoLife] = stream.map{ tuple => GeoLife(tuple) }
    
    val q0 = geoLifeStream
    	.assignAscendingTimestamps( geolife => geolife.timestamp.getTime )
      	.keyBy(0)
      	.timeWindow(Time.of(10, TimeUnit.MINUTES), Time.of(1, TimeUnit.MINUTES))
      	.apply { MobyDick.mobilePoint _ }
      	.filter( mo => mo.location.lengthAtTime(mo.location.endTime) > 3000 )
      	.map( mo => mo.location.atFinal.geom )

    q0.print
    
    env.execute("Apache Flink Geospatial")
  
  }

}