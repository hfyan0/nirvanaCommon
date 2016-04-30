package org.nirvana;

import java.io._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Period, DateTime, DateTimeZone, LocalDate, LocalTime, Duration, Days, Seconds}

//--------------------------------------------------
// case class can't have more than 22 columns
//--------------------------------------------------
case class MDIStruct(
    val dt:          DateTime,
    val symbol:      String,
    val tradeprice:  Double,
    val tradevolume: Long,
    val bidpv:       List[(Double, Long)],
    val askpv:       List[(Double, Long)]
) {

  override def toString: String = {
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
    b.append(symbol)
    b.append(",")
    b.append(tradeprice.toString)
    b.append(",")
    b.append(tradevolume.toString)
    b.append(",")
    b.append("B,")
    b.append(bidpv.map(tup => { tup._1.toString + "," + tup._2.toString }).mkString(","))
    b.append(",A,")
    b.append(askpv.map(tup => { tup._1.toString + "," + tup._2.toString }).mkString(","))

    b.toString
  }

}

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

  def toCashOHLCFeed(mkt: String, numLeadingZeros: Int): String = {
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
    if (numLeadingZeros > 0) b.append(SUtil.addLeadingChar(symbol, '0', numLeadingZeros))
    else b.append(symbol)
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

  private val _cash_datetime_fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss_000000")

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

    if (lscsvfields.length != 6 && lscsvfields.length != 7) return None
    if (!SUtil.isDouble(lscsvfields(2))) return None

    val dt_try1 = SUtil.convertTimestampFmt3(lscsvfields(0))
    val dt_try2 = SUtil.convertTimestampFmt4(lscsvfields(0))
    if (dt_try1 == None && dt_try2 == None) return None

    val dt = {
      if (dt_try1 != None) dt_try1.get
      else if (dt_try2 != None) dt_try2.get
      else SUtil.EPOCH
    }

    try {
      val ohlcpb = OHLCPriceBar(
        lscsvfields(2).toDouble,
        lscsvfields(3).toDouble,
        lscsvfields(4).toDouble,
        lscsvfields(5).toDouble,
        { if (lscsvfields.length == 6) 0 else lscsvfields(6).toDouble.toLong }
      )

      var sym = {
        val sym2 = lscsvfields(1).split(" ").toList
        if (sym2.length > 0)
          sym2(0)
        else
          lscsvfields(1)
      }

      if (addLeadingZeros && SUtil.isDouble(sym)) {
        sym = SUtil.addLeadingChar(sym, '0', 5)
      }

      return Some(OHLCBar(dt, sym, ohlcpb))
    }
    catch {
      case e: Exception => return None
    }

    return None
  }

  //--------------------------------------------------
  // 20160217_100000_000000,ohlcfeed,HKSE,00136,0.38,0.39,0.38,0.385,3084000
  // 20160217_100000_000000,ohlcfeed,HKSE,00138,0.87,0.87,0.87,0.87,38000
  // 20160217_100000_000000,ohlcfeed,HKSE,00139,0.37,0.37,0.365,0.37,420000
  // 20160217_100000_000000,ohlcfeed,HKSE,00141,7.0,7.0,6.93,6.93,6000
  // 20160217_100000_000000,ohlcfeed,HKSE,00142,5.48,5.48,5.43,5.44,130000
  // 20160217_100000_000000,ohlcfeed,HKSE,00143,0.295,0.295,0.28,0.285,11572000
  // 20160217_100000_000000,ohlcfeed,HKSE,00144,21.4,21.85,21.4,21.75,201840
  // 20160217_100000_000000,ohlcfeed,HKSE,00148,11.4,11.4,11.26,11.36,53500
  //--------------------------------------------------
  def parseCashOHLCFmt1(s: String, addLeadingZeros: Boolean): Option[OHLCBar] = {
    val lscsvfields = s.split(",").toList

    if (lscsvfields.length != 8 && lscsvfields.length != 9) return None
    if (!SUtil.isDouble(lscsvfields(4))) return None

    val dt_try = SUtil.convertTimestampFmt5(lscsvfields(0))

    val dt = dt_try match {
      case Some(t) => t
      case None    => SUtil.EPOCH
    }

    try {
      val ohlcpb = OHLCPriceBar(
        lscsvfields(4).toDouble,
        lscsvfields(5).toDouble,
        lscsvfields(6).toDouble,
        lscsvfields(7).toDouble,
        { if (lscsvfields.length == 8) 0 else lscsvfields(8).toDouble.toLong }
      )

      val sym =
        if (addLeadingZeros && SUtil.isDouble(lscsvfields(3)))
          SUtil.addLeadingChar(lscsvfields(3), '0', 5)
        else
          lscsvfields(3)

      return Some(OHLCBar(dt, sym, ohlcpb))
    }
    catch {
      case e: Exception => return None
    }

    return None
  }

  //--------------------------------------------------
  // 19900102_160000_000000,00941,80.1,400,B,80.1,1000,999999,0,999999,0,999999,0,999999,0,A,80.15,1000,999999,0,999999,0,999999,0,999999,0
  //--------------------------------------------------
  def parseCashMDI(s: String): Option[MDIStruct] = {
    val lscsvfields = s.split(",").toList

    if (lscsvfields.length != 26) return None
    if (!SUtil.isDouble(lscsvfields(2))) return None

    val dt = SUtil.convertTimestampFmt5(lscsvfields(0)) match {
      case Some(t) => t
      case None    => SUtil.EPOCH
    }

    try {

      val bidpv = (lscsvfields(5).toDouble, lscsvfields(6).toLong) ::
        (lscsvfields(7).toDouble, lscsvfields(8).toLong) ::
        (lscsvfields(9).toDouble, lscsvfields(10).toLong) ::
        (lscsvfields(11).toDouble, lscsvfields(12).toLong) ::
        (lscsvfields(13).toDouble, lscsvfields(14).toLong) :: Nil

      val askpv = (lscsvfields(16).toDouble, lscsvfields(17).toLong) ::
        (lscsvfields(18).toDouble, lscsvfields(19).toLong) ::
        (lscsvfields(20).toDouble, lscsvfields(21).toLong) ::
        (lscsvfields(22).toDouble, lscsvfields(23).toLong) ::
        (lscsvfields(24).toDouble, lscsvfields(25).toLong) :: Nil

      val mdis = MDIStruct(
        dt,
        lscsvfields(1),
        lscsvfields(2).toDouble,
        lscsvfields(3).toLong,
        bidpv,
        askpv
      )

      return Some(mdis)
    }
    catch {
      case e: Exception => return None
    }

    return None
  }

  //--------------------------------------------------
  // to Cash MDI format
  // 19900102_160000_000000,00941,80.1,400,B,80.1,1000,999999,0,999999,0,999999,0,999999,0,A,80.15,1000,999999,0,999999,0,999999,0,999999,0
  //--------------------------------------------------
  def convertToCashMDI(tt: TradeTick): String = {
    _cash_datetime_fmt.print(tt.dt) + "," + tt.symbol + "," + tt.price + "," + tt.volume + "," +
      "B," + tt.price + "," + tt.volume + ",999999,0,999999,0,999999,0,999999,0," +
      "A," + tt.price + "," + tt.volume + ",999999,0,999999,0,999999,0,999999,0"
  }

  //--------------------------------------------------
  // to Cash MDI format
  // 19900102_160000_000000,00941,80.1,400,B,80.1,1000,999999,0,999999,0,999999,0,999999,0,A,80.15,1000,999999,0,999999,0,999999,0,999999,0
  //--------------------------------------------------
  def convertFromOHLCBarToCashMDI(b: OHLCBar): String = {
    _cash_datetime_fmt.print(b.dt) + "," + b.symbol + "," + b.priceBar.c + "," + b.priceBar.v + "," +
      "B," + b.priceBar.c + "," + b.priceBar.v + ",999999,0,999999,0,999999,0,999999,0," +
      "A," + b.priceBar.c + "," + b.priceBar.v + ",999999,0,999999,0,999999,0,999999,0"
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
            outList += curBar.toCashOHLCFeed("HKSE", 5)
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
          outList += curBar.toCashOHLCFeed("HKSE", 5)
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

sealed abstract class TimeZone
case class HongKong extends TimeZone
case class NewYork extends TimeZone
case class London extends TimeZone

object SUtil {
  val EPSILON = 0.00001
  val SMALLNUM = 0.01
  val ATU_INVALID_PRICE = 999999.0

  val EPOCH = new DateTime(1970, 1, 1, 0, 0, 0)

  private val _dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def getCurrentSqlTimeStampStr(timezone: TimeZone): String = {
    getCurrentSqlTimeStamp(timezone).toString
  }

  def getCurrentDateTime(timezone: TimeZone): DateTime = {
    val utcnow = new DateTime(DateTimeZone.UTC)

    timezone match {
      case HongKong() => utcnow.plusHours(8)
      case NewYork()  => utcnow.plusHours(-5)
      case London()   => utcnow
      case _          => utcnow
    }
  }

  def getCurrentSqlTimeStamp(timezone: TimeZone): java.sql.Timestamp = {
    new java.sql.Timestamp(getCurrentDateTime(timezone).getMillis())
  }

  def getCurrentDateTimeStr(timezone: TimeZone): String = {
    getCurrentDateTime(timezone).toString
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
    SUtil.convertMySQLTSToDateTime(SUtil.convertTimestampFmt1(ts))
  }

  //--------------------------------------------------
  // from 2016-01-12 12:34 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt3(ts: String): Option[DateTime] = {
    if (ts.length < 16) return None
    else {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
      val dt: DateTime = formatter.parseDateTime(ts.substring(0, 16))
      return Some(dt)
    }
  }
  //--------------------------------------------------
  // from 2016-01-12 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt4(ts: String): Option[DateTime] = {
    if (ts.length < 10) return None
    else {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val dt: DateTime = formatter.parseDateTime(ts.substring(0, 10))
      return Some(dt)
    }
  }
  //--------------------------------------------------
  // from 20160112_144449_000000 to DateTime
  //--------------------------------------------------
  def convertTimestampFmt5(ts: String): Option[DateTime] = {
    if (ts.length < 22) return None
    else {
      val formatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss")
      val dt: DateTime = formatter.parseDateTime(ts.substring(0, 15))
      return Some(dt)
    }
  }

  def parseTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",").toList
    if (csvFields.length == 10) (true, csvFields.map(_.trim))
    else (false, List())
  }
  def parseAugmentedSignalFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",").toList
    if (csvFields.length == 10) (true, csvFields.map(_.trim))
    else (false, List())
  }
  def parseAugmentedTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",").toList
    if (csvFields.length == 11) (true, csvFields.map(_.trim))
    else (false, List())
  }
  def parseMarketFeedNominal(sMarketFeed: String): (Boolean, MarketFeedNominal) = {
    val csvFields = sMarketFeed.split(",").toList
    if (csvFields.length == 3 && isDouble(csvFields(2)))
      (true, MarketFeedNominal(SUtil.convertTimestampFmt2(csvFields(0).trim), csvFields(1).trim, csvFields(2).toDouble))
    else (false, MarketFeedNominal(SUtil.EPOCH, "", 0))
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

  def getListOfDatesWithinRange(startLD: LocalDate, endLD: LocalDate, mtmTime: LocalTime): List[DateTime] = {
    val startDT = startLD.toDateTime(mtmTime)
    val endDT = endLD.toDateTime(mtmTime)

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

  def calcReturns(p: List[Double]): List[Double] = {
    def calcReturnsImpl(lp: List[Double], c: Double): List[Double] = lp match {
      case Nil     => Nil
      case p :: ps => { val r = p / c; r } :: calcReturnsImpl(ps, p)
    }
    p match {
      case Nil     => Nil
      case x :: xs => calcReturnsImpl(xs, x)
    }
  }

  def safeToInt(in: String): Option[Int] = {
    try {
      Some(Integer.parseInt(in.trim))
    }
    catch {
      case e: NumberFormatException => None
    }
  }

  def safeToDouble(in: String): Option[Double] = {
    try {
      Some(in.trim.toDouble)
    }
    catch {
      case e: NumberFormatException => None
    }
  }

  def isAllDigits(x: String) = x forall Character.isDigit

  def calcCorrelMatrix(lslsReturns: List[List[Double]]): List[List[Double]] = {

    var rtn = List[List[Double]]()
    for (i <- 0 until lslsReturns.length) {
      var lsCorrelMatrixRow = List[Double]()
      for (j <- 0 until lslsReturns.length) {
        val subSet_i = lslsReturns(i)
        val subSet_j = lslsReturns(j)

        val mean_i = subSet_i.sum / subSet_i.length
        val mean_j = subSet_j.sum / subSet_j.length

        val dist_from_mean_i = subSet_i.map(_ - mean_i)
        val dist_from_mean_j = subSet_j.map(_ - mean_j)

        val var_i = dist_from_mean_i.map(x => x * x).sum / dist_from_mean_i.length
        val var_j = dist_from_mean_j.map(x => x * x).sum / dist_from_mean_j.length

        val dist_tup = dist_from_mean_i.zip(dist_from_mean_j)

        val scale = 1000
        val correl = Math.round(dist_tup.map(x => x._1 * x._2).sum /
          dist_tup.length / Math.sqrt(var_i) / Math.sqrt(var_j) * scale).toDouble / scale

        lsCorrelMatrixRow :+= correl
      }
      rtn :+= lsCorrelMatrixRow
    }

    rtn
  }

  def calcCovarMatrix(lslsReturns: List[List[Double]]): List[List[Double]] = {

    var rtn = List[List[Double]]()
    for (i <- 0 until lslsReturns.length) {
      var lsCovarMatrixRow = List[Double]()
      for (j <- 0 until lslsReturns.length) {
        val subSet_i = lslsReturns(i)
        val subSet_j = lslsReturns(j)

        val mean_i = subSet_i.sum / subSet_i.length
        val mean_j = subSet_j.sum / subSet_j.length

        val dist_from_mean_i = subSet_i.map(_ - mean_i)
        val dist_from_mean_j = subSet_j.map(_ - mean_j)

        val dist_tup = dist_from_mean_i.zip(dist_from_mean_j)

        val covar = dist_tup.map(x => x._1 * x._2).sum / dist_tup.length

        lsCovarMatrixRow :+= covar
      }
      rtn :+= lsCovarMatrixRow
    }

    rtn
  }

  def stdev(x: List[Double]): Double = {
    val m = x.sum / x.length
    Math.sqrt(x.map(d => (d - m) * (d - m)).sum / x.length)
  }

  // returns the cumulative normal distribution function (CNDF)
  // for a standard normal: N(0,1)
  // same as NORMSDIST in excel
  def CNDF(xa: Double): Double = {
    val neg: Int = if (xa < 0.0) 1 else 0;
    val x = if (neg == 1) xa * -1.0 else xa;

    val k: Double = (1d / (1d + 0.2316419 * x));
    var y: Double = ((((1.330274429 * k - 1.821255978) * k + 1.781477937) *
      k - 0.356563782) * k + 0.319381530) * k;
    y = 1.0 - 0.398942280401 * Math.exp(-0.5 * x * x) * y;

    return (1d - neg) * y + neg * (1d - y);
  }

}

class PeriodicTask(val intervalInSec: Int) {
  var _lastTime: DateTime = SUtil.EPOCH
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

object TradingHours {
  val defaultResponse = true

  def isTradingHour(symbol: String, dt: DateTime) = {
    val loctime = dt.toLocalTime
    val diff1 = Seconds.secondsBetween(loctime, new LocalTime(9, 30)).getSeconds
    val diff2 = Seconds.secondsBetween(loctime, new LocalTime(12, 0)).getSeconds
    val diff3 = Seconds.secondsBetween(loctime, new LocalTime(13, 0)).getSeconds
    val diff4 = Seconds.secondsBetween(loctime, new LocalTime(16, 0)).getSeconds

    if (diff1 > 0 || diff4 < 0) false
    else if (diff1 <= 0 && diff2 >= 0) true
    else if (diff2 < 0 && diff3 > 0) false
    else if (diff3 <= 0 && diff4 >= 0) true
    else false
  }
}
