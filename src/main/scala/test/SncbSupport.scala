package test

import java.sql.Timestamp

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import mobydick._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.windowing.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.Buffer

case class SncbEvent(
  deviceId: String,
  lon: Double,
  lat: Double,
  ts: Timestamp,
  gpsSpeed: Double,
  FA: Double,
  FF: Double,
  pointUTM: Point
)

object SncbEvent {

  private val geomFactory = new GeometryFactory()

  /**
    * Parse a CSV line into an SncbEvent.
    *
    * The field order matches GeoFlink.sncb.common.CSVToGpsEventMapFunction:
    *   0: ts (epoch millis)
    *   1: deviceId
    *   3: PCFA_mbar
    *   4: PCFF_mbar
    *   11: gps_speed
    *   12: lat
    *   13: lon
    */
  def fromCsv(line: String, delimiter: String = ","): SncbEvent = {
    val f = line.split(delimiter, -1)

    def parseLong(idx: Int): Long =
      try {
        f(idx).trim.toLong
      } catch {
        case _: Throwable => 0L
      }

    def parseDouble(idx: Int): Double =
      try {
        f(idx).trim.toDouble
      } catch {
        case _: Throwable => 0.0
      }

    val tsMillis = parseLong(0)
    val deviceId = if (f.length > 1) f(1) else ""
    val pcfa = if (f.length > 4) parseDouble(3) else 0.0
    val pcff = if (f.length > 5) parseDouble(4) else 0.0
    val gpsSpeed = if (f.length > 11) parseDouble(11) else 0.0
    val lat = if (f.length > 12) parseDouble(12) else 0.0
    val lon = if (f.length > 13) parseDouble(13) else 0.0

    val coordUTM: Coordinate = MobyDick.latlonToUTM(lat, lon)
    val pointUTM: Point = geomFactory.createPoint(coordUTM)

    SncbEvent(
      deviceId = deviceId,
      lon = lon,
      lat = lat,
      ts = new Timestamp(tsMillis),
      gpsSpeed = gpsSpeed,
      FA = pcfa,
      FF = pcff,
      pointUTM = pointUTM
    )
  }
}

case class VarAccumulator(
  count: Long,
  sumFA: Double,
  sumSqFA: Double,
  sumFF: Double,
  sumSqFF: Double
)

case class VarOut(
  start: Timestamp,
  end: Timestamp,
  varBP: Double,
  varFF: Double,
  cnt: Long
)

class SncbVarianceAgg extends AggregateFunction[SncbEvent, VarAccumulator, VarAccumulator] {

  override def createAccumulator(): VarAccumulator =
    VarAccumulator(0L, 0.0, 0.0, 0.0, 0.0)

  override def add(e: SncbEvent, acc: VarAccumulator): VarAccumulator = {
    val fa = e.FA
    val ff = e.FF
    VarAccumulator(
      count = acc.count + 1L,
      sumFA = acc.sumFA + fa,
      sumSqFA = acc.sumSqFA + fa * fa,
      sumFF = acc.sumFF + ff,
      sumSqFF = acc.sumSqFF + ff * ff
    )
  }

  override def getResult(acc: VarAccumulator): VarAccumulator = acc

  override def merge(a: VarAccumulator, b: VarAccumulator): VarAccumulator =
    VarAccumulator(
      count = a.count + b.count,
      sumFA = a.sumFA + b.sumFA,
      sumSqFA = a.sumSqFA + b.sumSqFA,
      sumFF = a.sumFF + b.sumFF,
      sumSqFF = a.sumSqFF + b.sumSqFF
    )
}

class SncbVarianceWindowFn
  extends ProcessWindowFunction[VarAccumulator, VarOut, String, TimeWindow] {

  override def process(
    key: String,
    ctx: SncbVarianceWindowFn#Context,
    elements: Iterable[VarAccumulator],
    out: Collector[VarOut]
  ): Unit = {
    val acc = elements.iterator.next()
    if (acc.count > 0L) {
      val n = acc.count.toDouble
      val meanFA = acc.sumFA / n
      val meanFF = acc.sumFF / n
      val varBP = acc.sumSqFA / n - meanFA * meanFA
      val varFF = acc.sumSqFF / n - meanFF * meanFF
      val start = new Timestamp(ctx.window.getStart)
      val end = new Timestamp(ctx.window.getEnd)
      out.collect(VarOut(start, end, varBP, varFF, acc.count))
    }
  }
}

case class TrajectoryOut(
  deviceId: String,
  start: Timestamp,
  end: Timestamp,
  trajectory: TemporalPoint
)

class SncbTrajectoryWindowFn
  extends WindowFunction[SncbEvent, TrajectoryOut, String, TimeWindow] {

  override def apply(
    key: String,
    window: TimeWindow,
    input: Iterable[SncbEvent],
    out: Collector[TrajectoryOut]
  ): Unit = {

    val events = input.toSeq.sortBy(_.ts.getTime)
    if (events.nonEmpty) {
      var intimePrev: IntimeObject = null
      val temporalPoint = new TemporalPoint(Buffer[PointUnit](), new TimePeriod(), null)

      events.foreach { e =>
        val g: Geometry = e.pointUTM
        val intimeCurr = new IntimeObject(e.ts, g)
        val upoint: PointUnit = MobyDick.createUpoint(intimePrev, intimeCurr)
        temporalPoint.addUnit(upoint)
        intimePrev = intimeCurr
      }

      val start = new Timestamp(window.getStart)
      val end = new Timestamp(window.getEnd)
      out.collect(TrajectoryOut(key, start, end, temporalPoint))
    }
  }
}

case class TrajSpeedOut(
  deviceId: String,
  start: Timestamp,
  end: Timestamp,
  trajectory: TemporalPoint,
  avgSpeed: Double,
  minSpeed: Double
)

class SncbTrajectorySpeedWindowFn
  extends ProcessWindowFunction[SncbEvent, TrajSpeedOut, String, TimeWindow] {

  override def process(
    key: String,
    ctx: SncbTrajectorySpeedWindowFn#Context,
    elements: Iterable[SncbEvent],
    out: Collector[TrajSpeedOut]
  ): Unit = {

    val events = elements.toSeq.sortBy(_.ts.getTime)
    if (events.nonEmpty) {
      var intimePrev: IntimeObject = null
      val temporalPoint = new TemporalPoint(Buffer[PointUnit](), new TimePeriod(), null)

      var count = 0L
      var sumSpeed = 0.0
      var minSpeed = Double.MaxValue

      events.foreach { e =>
        val g: Geometry = e.pointUTM
        val intimeCurr = new IntimeObject(e.ts, g)
        val upoint: PointUnit = MobyDick.createUpoint(intimePrev, intimeCurr)
        temporalPoint.addUnit(upoint)
        intimePrev = intimeCurr

        val s = e.gpsSpeed
        count += 1L
        sumSpeed += s
        if (s < minSpeed) {
          minSpeed = s
        }
      }

      if (count > 0L) {
        val avg = sumSpeed / count.toDouble
        val start = new Timestamp(ctx.window.getStart)
        val end = new Timestamp(ctx.window.getEnd)
        out.collect(TrajSpeedOut(key, start, end, temporalPoint, avg, minSpeed))
      }
    }
  }
}

object SncbGeometry {

  private val geomFactory = new GeometryFactory()
  private val wktReader = new WKTReader()

  /**
    * Convert a WKT polygon expressed in lon/lat (EPSG:4326) into
    * a Polygon whose coordinates are in UTM, using MobyDick.latlonToUTM.
    */
  def polygonWktToUtm(wkt: String): Polygon = {
    val geom = wktReader.read(wkt)
    geom match {
      case poly: Polygon =>
        val coords = poly.getExteriorRing.getCoordinates
        val utmCoords: Array[Coordinate] =
          coords.map(c => MobyDick.latlonToUTM(c.y, c.x))
        val ring: LinearRing = geomFactory.createLinearRing(utmCoords)
        geomFactory.createPolygon(ring, null)
      case _ =>
        throw new IllegalArgumentException("Polygon WKT expected")
    }
  }
}

