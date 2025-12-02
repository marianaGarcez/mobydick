package test

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SncbQ4_TrajectoryRestricted {

  /**
    * Q4 – Trajectory creation in restricted STBOX.
    *
    * MobilityNebula equivalent:
    *   SELECT start, end,
    *          TEMPORAL_SEQUENCE(gps_lon, gps_lat, time_utc) AS trajectory
    *   FROM sncb_stream
    *   WHERE TGeo_AT_STBOX(...) = 1
    *   WINDOW SLIDING(time_utc, SIZE 20 SEC, ADVANCE BY 2 SEC)
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

    val minLon = 4.0
    val maxLon = 4.6
    val minLat = 50.0
    val maxLat = 50.8
    val tMin = Timestamp.valueOf("2024-08-01 00:00:00")
    val tMax = Timestamp.valueOf("2025-08-01 00:00:00")

    val filtered = sncbStream.filter { e =>
      e.lon >= minLon && e.lon <= maxLon &&
      e.lat >= minLat && e.lat <= maxLat &&
      !e.ts.before(tMin) && !e.ts.after(tMax)
    }

    val trajectories = filtered
      .keyBy(_.deviceId)
      .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(2)))
      .apply(new SncbTrajectoryWindowFn)

    trajectories.print()

    env.execute("SNCB Q4 - Trajectory in STBOX")
  }
}
