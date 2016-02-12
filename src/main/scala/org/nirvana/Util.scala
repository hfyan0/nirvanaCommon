package org.nirvana;

import java.io._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Period, DateTime, Duration, Days}

case class OHLCPriceBar(val o: Double, val h: Double, val l: Double, val c: Double, val v: Long)
case class OHLCBar(val dt: DateTime, val symbol: String, val priceBar: OHLCPriceBar) {

  override def toString: String = {
    var b = new StringBuilder
    b.append(symbol)
    b.append(",")
    b.append(dt.getYear().toString)
    b.append("-")
    b.append(String.format("%2s", dt.getMonthOfYear().toString).replace(' ', '0'))
    b.append("-")
    b.append(String.format("%2s", dt.getDayOfMonth().toString).replace(' ', '0'))
    b.append(",")
    b.append(String.format("%2s", dt.getHourOfDay().toString).replace(' ', '0'))
    b.append(":")
    b.append(String.format("%2s", dt.getMinuteOfHour().toString).replace(' ', '0'))
    b.append(":")
    b.append(String.format("%2s", dt.getSecondOfMinute().toString).replace(' ', '0'))
    b.append(",")
    b.append(priceBar.o.toString)
    b.append(",")
    b.append(priceBar.h.toString)
    b.append(",")
    b.append(priceBar.l.toString)
    b.append(",")
    b.append(priceBar.c.toString)
    b.append(",")
    b.append(priceBar.v.toString)
    b.toString
  }

  def toOHLCFeed(mkt: String, numLeadingZeros: Int): String = {
    //--------------------------------------------------
    // ohlcfeed
    // 20110314_093500_000000,ohlcfeed,HKSE,28992,0.175,0.175,0.175,0.175,520000
    //--------------------------------------------------
    var b = new StringBuilder
    b.append(dt.getYear().toString)
    b.append(String.format("%2s", dt.getMonthOfYear().toString).replace(' ', '0'))
    b.append(String.format("%2s", dt.getDayOfMonth().toString).replace(' ', '0'))
    b.append("_")
    b.append(String.format("%2s", dt.getHourOfDay().toString).replace(' ', '0'))
    b.append(String.format("%2s", dt.getMinuteOfHour().toString).replace(' ', '0'))
    b.append(String.format("%2s", dt.getSecondOfMinute().toString).replace(' ', '0'))
    b.append("_000000")
    b.append(",")
    b.append("ohlcfeed")
    b.append(",")
    b.append(mkt)
    b.append(",")
    if (numLeadingZeros > 0) {
      b.append(Util.addLeadingChar(symbol, '0', numLeadingZeros))
    }
    b.append(",")
    b.append(priceBar.o.toString)
    b.append(",")
    b.append(priceBar.h.toString)
    b.append(",")
    b.append(priceBar.l.toString)
    b.append(",")
    b.append(priceBar.c.toString)
    b.append(",")
    b.append(priceBar.v.toString)
    b.toString
  }

}

case class TradeTick(val dt: DateTime, val symbol: String, val price: Double, val volume: Long)

class OHLCAcc {
  var _o: Double = -1
  var _h: Double = -1
  var _l: Double = -1
  var _c: Double = -1
  var _v: Long = 0

  def updateOHLC(tickPrice: Double, volume: Long) {
    if (_o < 0) {
      _o = tickPrice
      _h = tickPrice
      _l = tickPrice
      _c = tickPrice
      _v = volume
    }
    else {
      if (tickPrice > _h) _h = tickPrice
      if (tickPrice < _l) _l = tickPrice
      _c = tickPrice
      _v += volume
    }
    Unit
  }
  def getOHLCPriceBar: OHLCPriceBar = new OHLCPriceBar(_o, _h, _l, _c, _v)

  def reset {
    _o = _c
    _h = _c
    _l = _c
    _v = 0
  }
  def ready: Boolean = {
    if (_o < 0) false
    else true
  }
}

object DataFmtAdaptors {

  def parseHKExFmt1(s: String): TradeTick = {
    // 08002  8003630   5.290      11000U09200020140102 0
    // 08002  8003630   5.290       5000U09200020140102 0
    // 08002  8003630   5.290       4000U09200020140102 0

    val symbol = s.substring(0, 5)
    val price = s.substring(14, 22).trim.toDouble
    val volume = s.substring(22, 33).trim.toLong

    val year = s.substring(40, 44)
    val month = s.substring(44, 46)
    val day = s.substring(46, 48)
    val hour = s.substring(34, 36)
    val minute = s.substring(36, 38)
    val sec = s.substring(38, 40)
    val dt = new DateTime(year.toInt, month.toInt, day.toInt, hour.toInt, minute.toInt, sec.toInt)

    new TradeTick(dt, symbol, price, volume)

    // pw.write(symbol.toString)
    // pw.write(",")
    // pw.write(price.toString)
    // pw.write(",")
    // pw.write(volume.toString)
    // pw.write(",")
    // pw.write(dt.toString)
    // pw.write("\n")
  }

  //--------------------------------------------------
  // 2016-02-08,FDN US Equity,58.58,58.7389,56.73,57.9,3034930.0
  // 2016-02-09,FDN US Equity,56.99,59.195,56.6,57.33,2333350.0
  // 2016-02-10,FDN US Equity,58.27,59.72,58.05,58.21,1201957.0
  // 2016-02-11,FDN US Equity,57.58,59.2899,57.39,58.84,1360557.0
  // convert the "FDN US Equity" to "FDN" as well
  //--------------------------------------------------
  def parseBlmgFmt1(s: String, addLeadingZeros: Boolean): Option[OHLCBar] = {
    val lscsvfields = s.split(",").toList

    if (!Util.isDouble(lscsvfields(2)) ||
      lscsvfields.length != 7) None
    else {
      val dt_try1 = Util.convertTimestampFmt3(lscsvfields(0))
      val dt_try2 = Util.convertTimestampFmt4(lscsvfields(0))

      if (dt_try1 == None && dt_try2 == None) None
      val dt = {
        if (dt_try1 != None) dt_try1.get
        else if (dt_try2 != None) dt_try2.get
        else Util.EPOCH
      }

      val ohlcpb = OHLCPriceBar(
        lscsvfields(2).toDouble,
        lscsvfields(3).toDouble,
        lscsvfields(4).toDouble,
        lscsvfields(5).toDouble,
        lscsvfields(6).toDouble.toLong
      )
      var sym = {
        val sym2 = lscsvfields(1).split(" ").toList
        if (sym2.length > 0)
          sym2(0)
        else
          lscsvfields(1)
      }

      if (addLeadingZeros) {
        sym = Util.addLeadingChar(sym, '0', 5)
      }

      Some(OHLCBar(dt, sym, ohlcpb))
    }
  }

  class OHLCOutputAdaptor(val startTime: DateTime, val endTime: DateTime, val barIntervalInSec: Int) {

    var ohlcacc = new OHLCAcc()
    var prev_bar_dt = startTime
    var prev_bar_sym = ""

    def convertToOHLCOutput(tradetick: TradeTick): List[String] = {

      var outList = scala.collection.mutable.ListBuffer[String]()

      if (prev_bar_sym != tradetick.symbol && prev_bar_sym != "") {
        //--------------------------------------------------
        // finish off the previous symbol first
        // illiquid stocks may not have trades just before market close
        //--------------------------------------------------
        while (prev_bar_dt.getSecondOfDay < endTime.getSecondOfDay) {
          prev_bar_dt = prev_bar_dt.plusSeconds(barIntervalInSec)
          val curPriceBar = ohlcacc.getOHLCPriceBar
          val curBar = new OHLCBar(prev_bar_dt, prev_bar_sym, curPriceBar)
          if (ohlcacc.ready &&
            prev_bar_dt.getSecondOfDay <= endTime.getSecondOfDay) {
            // outList += curBar.toString
            outList += curBar.toOHLCFeed("HKSE", 5)
          }
          ohlcacc.reset
        }

        //--------------------------------------------------
        // reset internal data struct
        //--------------------------------------------------
        prev_bar_dt = startTime
        prev_bar_sym = tradetick.symbol
        ohlcacc = new OHLCAcc()

      }

      //--------------------------------------------------
      // ok, deal with the current trade tick for current symbol
      //--------------------------------------------------
      var timeDiffInSec = tradetick.dt.getSecondOfDay - prev_bar_dt.getSecondOfDay

      //--------------------------------------------------
      // need to seal previous bars
      //--------------------------------------------------
      while (timeDiffInSec > barIntervalInSec
        &&
        prev_bar_dt.getSecondOfDay < endTime.getSecondOfDay) {
        prev_bar_dt = prev_bar_dt.plusSeconds(barIntervalInSec)
        val curPriceBar = ohlcacc.getOHLCPriceBar
        val curBar = new OHLCBar(prev_bar_dt, prev_bar_sym, curPriceBar)
        if (ohlcacc.ready) {
          // outList += curBar.toString
          outList += curBar.toOHLCFeed("HKSE", 5)
        }
        ohlcacc.reset
        timeDiffInSec = tradetick.dt.getSecondOfDay - prev_bar_dt.getSecondOfDay

      }
      ohlcacc.updateOHLC(tradetick.price, tradetick.volume)
      prev_bar_sym = tradetick.symbol
      outList.toList

    }

  }
}

case class TradeFeed(
  val datetime:        DateTime,
  val symbol:          String,
  val trade_price:     Double,
  val trade_volume:    Double,
  val trade_sign:      Int,
  val signed_volume:   Double,
  val signed_notional: Double
)

case class MarketFeedNominal(
  val datetime:      DateTime,
  val symbol:        String,
  val nominal_price: Double
)

case class PnLCalcRow(
  val trdPx:                Double,
  val trdVol:               Double,
  val trdSgn:               Int,
  val cumSgndVol:           Double,
  val cumSgndNotlAdjChgSgn: Double,
  val avgPx:                Double,
  val prdTotPnL:            Double,
  val cumTotPnL:            Double,
  val psnClosed:            Double,
  val rlzdPnL:              Double,
  val cumRlzdPnL:           Double,
  val cumUrlzdPnL:          Double
)

object Util {
  val EPSILON = 0.00001
  val SMALLNUM = 0.01
  val EPOCH = new DateTime(1970, 1, 1, 0, 0, 0)

  private val _dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def getCurrentTimeStamp(): java.sql.Timestamp = {
    val today = new java.util.Date()
    new java.sql.Timestamp(today.getTime())
  }
  def getCurrentTimeStampStr(): String = {
    getCurrentTimeStamp.toString
  }
  def getCurrentDateTime(): DateTime = {
    new DateTime()
  }
  def getCurrentDateTimeStr(): String = {
    getCurrentDateTime.toString
  }

  def convertDateTimeToStr(dt: DateTime): String = {
    _dateTimeFormat.print(dt)
  }
  //--------------------------------------------------
  // from 20160112_123456_000000 to 2016-01-12 12:34:56.000000
  //--------------------------------------------------
  def convertTimestampFmt1(ts: String): String = {
    val tsFields = ts.split("_")
    if (tsFields.length != 3) ""
    else {
      val sYYYYMMDD = tsFields(0)
      val sHHMMSS = tsFields(1)
      val sMillisec = tsFields(2)

      val sYear = sYYYYMMDD.substring(0, 4)
      val sMonth = sYYYYMMDD.substring(4, 6)
      val sDay = sYYYYMMDD.substring(6, 8)

      val sHour = sHHMMSS.substring(0, 2)
      val sMin = sHHMMSS.substring(2, 4)
      val sSec = sHHMMSS.substring(4, 6)

      sYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin + ":" + sSec + "." + sMillisec
    }
  }

  //--------------------------------------------------
  // from MySQL timestamp in the format 
  //--------------------------------------------------
  def convertMySQLTSToDateTime(ts: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dt: DateTime = formatter.parseDateTime(ts.substring(0, 19))
    dt
  }

  //--------------------------------------------------
  // from 20160112_123456_000000 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt2(ts: String): DateTime = {
    Util.convertMySQLTSToDateTime(Util.convertTimestampFmt1(ts))
  }

  //--------------------------------------------------
  // from 2016-01-12 12:34 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt3(ts: String): Option[DateTime] = {
    if (ts.length < 16) None
    else {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
      val dt: DateTime = formatter.parseDateTime(ts.substring(0, 16))
      Some(dt)
    }
  }
  //--------------------------------------------------
  // from 2016-01-12 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt4(ts: String): Option[DateTime] = {
    if (ts.length < 10) None
    else {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val dt: DateTime = formatter.parseDateTime(ts.substring(0, 10))
      Some(dt)
    }
  }

  def parseTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",")
    if (csvFields.length == 10) (true, csvFields.toList)
    else (false, List())
  }
  def parseAugmentedTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",")
    if (csvFields.length == 11) (true, csvFields.toList)
    else (false, List())
  }
  def parseMarketFeedNominal(sMarketFeed: String): (Boolean, MarketFeedNominal) = {
    val csvFields = sMarketFeed.split(",")
    if (csvFields.length == 3 && isDouble(csvFields(2))) (true, MarketFeedNominal(Util.convertTimestampFmt2(csvFields(0)), csvFields(1), csvFields(2).toDouble))
    else (false, MarketFeedNominal(Util.EPOCH, "", 0))
  }

  def isDouble(x: String) = {
    try {
      x.toDouble
      true
    }
    catch {
      case e: NumberFormatException => false
    }
  }

  def getListOfDatesWithinRange(startDT: DateTime, endDT: DateTime): List[DateTime] = {
    val numofdays = Days.daysBetween(startDT, endDT).getDays()
    (0 to numofdays).map(startDT.plusDays(_)).toList
  }

  def getFilesInDir(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    }
    else {
      List[File]()
    }
  }

  def addLeadingChar(s: String, c: Char, len: Int): String = {
    if (len > s.length) {
      ((for (i <- (1 to (len - s.length)).map(_ => '0')) yield (i))(collection.breakOut) + s)
    }
    else
      s

  }

}

class PeriodicTask(val intervalInSec: Int) {
  var _lastTime: DateTime = Util.EPOCH
  def checkIfItIsTimeToWakeUp(timeNow: DateTime): Boolean = {
    val msAfter = timeNow.getMillis - _lastTime.getMillis
    val secAfter = msAfter / 1000
    if (secAfter >= intervalInSec) {
      _lastTime = _lastTime.plusSeconds((secAfter / intervalInSec).toInt * intervalInSec)
      true
    }
    else false

  }
}
