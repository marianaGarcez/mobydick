package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SncbQ2_BrakeMonitor {

  /**
    * Q2 – Brake monitoring with variance (sliding 10s window, 200ms slide).
    *
    * MobilityNebula equivalent:
    *   - filter out points intersecting a maintenance polygon
    *   - WINDOW SLIDING(time_utc, SIZE 10 SEC, ADVANCE BY 200 MS)
    *   - compute VAR(PCFA_mbar) and VAR(PCFF_mbar)
    *   - keep windows with varBP > 0.1 and varFF <= 10
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)

    val sncbStream: DataStream[SncbEvent] =
      stream
        .map(line => SncbEvent.fromCsv(line))
        .assignAscendingTimestamps(e => e.ts.getTime)

    val maintenanceWkt =
      "POLYGON((4.0 50.0, 4.0 50.8, 4.6 50.8, 4.6 50.0, 4.0 50.0))"

    val maintenancePolygon = SncbGeometry.polygonWktToUtm(maintenanceWkt)

    val filtered = sncbStream.filter { e =>
      !maintenancePolygon.intersects(e.pointUTM)
    }

    val result = filtered
      .keyBy(_.deviceId)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(200)))
      .aggregate(new SncbVarianceAgg, new SncbVarianceWindowFn)
      .filter(v => v.varBP > 0.1 && v.varFF <= 10.0)

    result.print()

    env.execute("SNCB Q2 - Brake monitoring with variance")
  }
}
