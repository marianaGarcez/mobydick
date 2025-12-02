package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SncbQ3_Trajectory {

  /**
    * Q3 – Trajectory creation (sliding 3s window, 1s slide).
    *
    * MobilityNebula equivalent:
    *   SELECT start, end,
    *          TEMPORAL_SEQUENCE(gps_lon, gps_lat, time_utc) AS trajectory
    *   FROM sncb_stream
    *   WINDOW SLIDING(time_utc, SIZE 3 SEC, ADVANCE BY 1 SEC)
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

    val trajectories = sncbStream
      .keyBy(_.deviceId)
      .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
      .apply(new SncbTrajectoryWindowFn)

    trajectories.print()

    env.execute("SNCB Q3 - Trajectory (sliding)")
  }
}
