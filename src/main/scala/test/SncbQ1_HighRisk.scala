package test

import java.sql.Timestamp

import com.vividsolutions.jts.geom._
import mobydick._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SncbQ1_HighRisk {

  /**
    * Q1 – High‑risk point proximity (tumbling 5s window).
    *
    * Equivalent to the MobilityNebula query:
    *   SELECT start, end, COUNT(time_utc) AS cnt
    *   FROM sncb_stream
    *   WHERE EDWITHIN(..., 2m) = 1
    *   WINDOW TUMBLING(time_utc, SIZE 5 SEC)
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)

    val sncbStream: DataStream[SncbEvent] =
      stream.map(line => SncbEvent.fromCsv(line))

    val geomFactory = new GeometryFactory()
    val poiCoord: Coordinate = MobyDick.latlonToUTM(50.6456, 4.3658)
    val poi: Point = geomFactory.createPoint(poiCoord)

    val result = sncbStream
      .assignAscendingTimestamps(e => e.ts.getTime)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(
        new ProcessAllWindowFunction[SncbEvent, (Timestamp, Timestamp, Long), TimeWindow] {
          override def process(
            context: ProcessAllWindowFunction[SncbEvent, (Timestamp, Timestamp, Long), TimeWindow]#Context,
            elements: Iterable[SncbEvent],
            out: Collector[(Timestamp, Timestamp, Long)]
          ): Unit = {
            val cnt = elements.count(e => e.pointUTM.distance(poi) <= 2.0)
            if (cnt > 0L) {
              val start = new Timestamp(context.window.getStart)
              val end = new Timestamp(context.window.getEnd)
              out.collect((start, end, cnt))
            }
          }
        }
      )

    result.print()

    env.execute("SNCB Q1 - High-risk point proximity")
  }
}
