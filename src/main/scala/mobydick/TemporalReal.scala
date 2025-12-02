package mobydick

import scala.util.control._
import java.sql.Timestamp
import scala.collection.mutable.Buffer
import java.util.Date



case class TemporalReal(u: Buffer[RealUnit]) { 
    private var _units: Buffer[RealUnit] = u
    
    def units = _units
    
    def units_= (value: Buffer[RealUnit]): Unit = _units = value
    
    def min(): Double = {
        val loop = new Breaks
        var upArray: Buffer[RealUnit] = this.units
        val noUnits: Int = upArray.length
        var up: RealUnit = upArray(0)
        var minValue: Double = up.localMin()
        loop.breakable{
          for(i <- 1 to noUnits-1 by 1) {
            up = upArray(i)
            if(minValue > up.localMin()) {
              minValue = up.localMin()
            }
          }
        }
        minValue
    }
    
    def max(): Double = {
        val loop = new Breaks
        var upArray: Buffer[RealUnit] = this.units
        val noUnits: Int = upArray.length
        var up: RealUnit = upArray(0)
        var maxValue: Double = up.localMax()
        loop.breakable{
          for(i <- 1 to noUnits-1 by 1) {
            up = upArray(i)
            if(maxValue < up.localMax()) {
              maxValue = up.localMax()
            }
          }
        }
        maxValue
    }
    
    def valueAtTime(t: Timestamp): Double = {
      /*
       * 
       */
        var found: Boolean = false
        val loop = new Breaks
        var upArray: Buffer[RealUnit] = this.units
        val noUnits: Int = upArray.length
        var up: RealUnit = upArray(0)
        loop.breakable{
          for(i <- 0 to noUnits-1 by 1) {
            up = upArray(i)
            var begin: Timestamp = up.period.start
            var end: Timestamp = up.period.end
            if(t.getTime() < begin.getTime()) loop.break
            if(up.period.leftClosed) {
              if(t.getTime() == begin.getTime()) {
                found = true
                loop.break
              }
            }
            if(up.period.rightClosed) {
              if(t.getTime() == end.getTime()) {
                found = true
                loop.break
              }
            }
            if((t.getTime() > begin.getTime()) && (t.getTime() < end.getTime())) {
              found = true
              loop.break
            }
          }
        }
        var retValue: Double = 0.0
        if(found) {
          retValue = up.a * t.getTime()/1000*t.getTime()/1000 + up.b * t.getTime()/1000 + up.c
        }
        return retValue
    }
    
    def timeAtValue(value: Double): Timestamp = {
      /*
       * TO DO
       */
      new Timestamp((new Date()).getTime())
    }
    
}