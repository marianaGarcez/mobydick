package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SncbQ5_TrajAndSpeedFence {

  /**
    * Q5 – Geofenced trajectory + speed aggregates.
    *
    * MobilityNebula equivalent:
    *   SELECT device_id, start, end,
    *          TEMPORAL_SEQUENCE(gps_lon, gps_lat, time_utc) AS trajectory,
    *          AVG(gps_speed) AS avg_speed,
    *          MIN(gps_speed) AS min_speed
    *   FROM sncb_stream
    *   WHERE EDWITHIN_TGEO_GEO(..., 1.0) = 1
    *   GROUP BY device_id
    *   WINDOW SLIDING(time_utc, SIZE 20 SEC, ADVANCE BY 2 SEC)
    *   HAVING (avg_speed < 100.0) OR (min_speed < 20.0)
    *   INTO file_sink;
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)

    val sncbStream: DataStream[SncbEvent] =
      stream
        .map(line => SncbEvent.fromCsv(line))
        .assignAscendingTimestamps(e => e.ts.getTime)

    val fenceWkt =
      "POLYGON((4.32 50.60, 4.32 50.72, 4.48 50.72, 4.48 50.60, 4.32 50.60))"

    val fencePolygon = SncbGeometry.polygonWktToUtm(fenceWkt)

    val inFence = sncbStream.filter { e =>
      fencePolygon.distance(e.pointUTM) <= 1.0
    }

    val trajSpeed = inFence
      .keyBy(_.deviceId)
      .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(2)))
      .process(new SncbTrajectorySpeedWindowFn)
      .filter(o => o.avgSpeed < 100.0 || o.minSpeed < 20.0)

    trajSpeed.print()

    env.execute("SNCB Q5 - Trajectory and speed in fence")
  }
}
