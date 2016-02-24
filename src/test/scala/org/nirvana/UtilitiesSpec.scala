import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import scala.util.Random
import org.joda.time.{Period, DateTime, Duration}
import org.nirvana._

class UtilTest extends AssertionsForJUnit {

  @Before def initialize() {
    println("Initializing tests.")
  }

  @Test def testUpdateOHCL() {

    //--------------------------------------------------
    // testing with random numbers
    //--------------------------------------------------
    val r = new scala.util.Random(2357)
    val ohlc2 = new OHLCAcc()
    assertFalse(ohlc2.ready)

    var lastc = -1D
    for (j <- 0 to 10000) {
      val s = scala.collection.mutable.Set[Double]()
      var o = -1D
      var c = -1D
      var v = 0L
      ohlc2.reset
      for (i <- 0 to 50) {
        val rand_price = r.nextInt(1000).toDouble
        val rand_volume = r.nextInt(1000).toLong
        if (o < 0) {
          if (lastc < 0) o = rand_price
          else {
            o = lastc
            s += lastc
          }
        }
        c = rand_price
        ohlc2.updateOHLC(rand_price, rand_volume)
        s += rand_price
        v += rand_volume
      }
      lastc = c
      assertEquals(ohlc2.getOHLCPriceBar, new OHLCPriceBar(o, s.max, s.min, c, v))
    }

  }

  @Test def testOutputOHLCFeed() {
    val ohlcb = OHLCBar(new DateTime(2016, 1, 15, 12, 30, 6), "941", OHLCPriceBar(90, 100, 80, 85, 7))
    assertEquals(ohlcb.toOHLCFeed("HKSE", 5), "20160115_123006_000000,ohlcfeed,HKSE,00941,90.0,100.0,80.0,85.0,7")
  }

  @Test def testConvertTimestamp() {
    assertEquals(SUtil.convertTimestampFmt1("20151216_045514_142013"), "2015-12-16 04:55:14.142013")
    assertEquals(SUtil.convertTimestampFmt1("20151216_145514_000000"), "2015-12-16 14:55:14.000000")
    assertEquals(SUtil.convertTimestampFmt3("2016-02-12 16:59"), Some(new DateTime(2016, 2, 12, 16, 59, 0)))
    assertEquals(SUtil.convertTimestampFmt4("2016-02-12"), Some(new DateTime(2016, 2, 12, 0, 0, 0)))
  }

  @Test def testIsFloat() {
    assert(SUtil.isDouble("1.4"))
    assert(SUtil.isDouble("14"))
    assert(SUtil.isDouble("-1"))
    assert(SUtil.isDouble("-3.14"))
    assertFalse(SUtil.isDouble("1.4a"))
    assertFalse(SUtil.isDouble("abc"))
    assertFalse(SUtil.isDouble("a1.4"))
  }

  @Test def testPeriodicTask() {
    val pt = new PeriodicTask(3)
    assert(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 3)))
    assertFalse(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 4)))
    assertFalse(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 5)))
    assert(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 6)))
    assertFalse(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 7)))
    assertFalse(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 8)))
    assert(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 2, 0, 2, 9)))
    assert(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 3, 0, 2, 9)))
    assertFalse(pt.checkIfItIsTimeToWakeUp(new DateTime(2016, 1, 3, 0, 2, 9)))
  }

  @Test def testGetFilesInDir() {
    val lsf = SUtil.getFilesInDir("/tmp")
    assertTrue(lsf.length > 0)
  }

  @Test def testAddLeadingChar() {
    assertEquals(SUtil.addLeadingChar("16", '0', 5), "00016")
    assertEquals(SUtil.addLeadingChar("", '0', 5), "00000")
    assertEquals(SUtil.addLeadingChar("333333", '0', 5), "333333")
  }

  @Test def testGetCurrentTime() {
    println("HongKong: " + SUtil.getCurrentSqlTimeStampStr(HongKong()))
    println("HongKong: " + SUtil.getCurrentDateTimeStr(HongKong()))
    println("NewYork: " + SUtil.getCurrentSqlTimeStampStr(NewYork()))
    println("NewYork: " + SUtil.getCurrentDateTimeStr(NewYork()))

    assertEquals(SUtil.getCurrentSqlTimeStampStr(HongKong()).substring(11,19),SUtil.getCurrentDateTimeStr(HongKong()).substring(11,19))
    assertEquals(SUtil.getCurrentSqlTimeStampStr(NewYork()).substring(11,19),SUtil.getCurrentDateTimeStr(NewYork()).substring(11,19))
    assertEquals(SUtil.getCurrentSqlTimeStampStr(London()).substring(11,19),SUtil.getCurrentDateTimeStr(London()).substring(11,19))
  }

  @Test def testTradingHours() {
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 8, 29, 0)), false)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 9, 29, 0)), false)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 12, 0, 1)), false)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 12, 59, 59)), false)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 16, 0, 1)), false)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 18, 0, 1)), false)

    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 9, 30, 0)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 9, 30, 1)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 11, 59, 59)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 12, 0, 0)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 13, 0, 0)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 13, 0, 1)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 15, 59, 59)), true)
    assertEquals(TradingHours.isTradingHour("Dummy", new DateTime(2016, 1, 20, 16, 0, 0)), true)
  }

}
