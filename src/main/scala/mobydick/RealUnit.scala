package mobydick

import scala.util.control._

case class RealUnit(firstParam: Double, secondParam: Double, thirdParam: Double, p: TimeInterval) {
    private var _a: Double = firstParam
    private var _b: Double = secondParam
    private var _c: Double = thirdParam
    
    private var _tInterval: TimeInterval = p
    
    def a = _a
    def b = _b
    def c = _c
    def period = _tInterval
    
    def a_= (value: Double):Unit = _a = value
    def b_= (value: Double):Unit = _b = value
    def c_= (value: Double):Unit = _c = value

    def period_= (value: TimeInterval):Unit = _tInterval = value
    
    def localMin(): Double = {
      val loop = new Breaks
      var startT: Long = this.period.start.getTime()
      var endT: Long = this.period.end.getTime()
      var localMinValue: Double = this.a*startT*startT + this.b*startT + this.c
      loop.breakable{
        for(t <- startT + 1000 to endT by 1000) {
          val resultMinValue: Double = this.a*t*t + b*t + c
          if(localMinValue > resultMinValue) {
            localMinValue = resultMinValue
          }
        }
      }
      localMinValue
    }
    
    def localMax(): Double = {
      val loop = new Breaks
      var startT: Long = this.period.start.getTime()
      var endT: Long = this.period.end.getTime()
      var localMaxValue: Double = this.a*startT*startT + this.b*startT + this.c
      loop.breakable{
        for(t <- startT + 1000 to endT by 1000) {
          val resultMinValue: Double = this.a*t/1000*t/1000 + b*t/1000 + c
          if(localMaxValue < resultMinValue) {
            localMaxValue = resultMinValue
          }
        }
      }
      localMaxValue
    }
}