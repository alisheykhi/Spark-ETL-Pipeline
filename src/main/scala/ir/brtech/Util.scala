package ir.brtech

import java.text.SimpleDateFormat

import com.github.sbahmani.jalcal.util.JalCal
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, trim, udf, when}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Column, Row}
import org.ini4j.Profile
import java.math.{BigDecimal, BigInteger, MathContext, RoundingMode}
import scala.annotation.tailrec
import scala.math.BigDecimal.javaBigDecimal2bigDecimal
import javax.xml.bind.DatatypeConverter

object Util {

  val jalaliToGregorianUDF: UserDefinedFunction = udf[String, String](time => {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateTime = JalCal.jalaliToGregorian(
      time.slice(0, 4).toInt,
      time.slice(4, 6).toInt,
      time.slice(6, 8).toInt,
      time.slice(8, 10).toInt,
      time.slice(10, 12).toInt,
      time.slice(12, 14).toInt
    )
    dateFormat.format(dateTime)
  })


  val gregorianToJalaliUDF: UserDefinedFunction = udf[String, String] { time =>
    {
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      dateFormat.parse(time)
      JalCal
        .gregorianToJalaliDate(dateFormat.getCalendar.getTime, false)
        .replace("/", "")
    }
  }

  def nvl(ColIn: Column, ReplaceVal: Any): Column = {
    when(ColIn.isNull, lit(ReplaceVal)).otherwise(ColIn)
  }
  def nvlOrEmpty(ColIn: Column, ReplaceVal: Any): Column = {
    when(ColIn.isNull, lit(ReplaceVal))
      .when(trim(ColIn) === "", lit(ReplaceVal))
      .otherwise(ColIn)
  }


  def sha1toIntString = udf((hex: String) => {
    val dec =Decimal(new java.math.BigInteger(hex.toUpperCase, 16)).toString
//    val i = if (dec.length % 2 == 0) 40 else 39
    val i = dec.length match {
      case x if x % 2 == 0 && x >= 42 => 40
      case x if x >= 41 => 39
      case x => x - 1
    }
    val f = BigInt(dec.substring(0,i)).+ {
      dec.substring(i,i+1).toInt match {
        case x if x >= 5 => 1
        case _ => 0
      }
    }.toString ++ dec.substring(i)
    List(i until f.length :_*).foldRight(f)((s, idx) => idx.updated(s, '0'))
  }
  )

  def scientificNotation = udf((numberText: String) => {
    numberText.length match {
      case x if x >= 40 => {
        val formatter  = new java.text.DecimalFormat("0.00E00")
        formatter.setRoundingMode(java.math.RoundingMode.HALF_UP)
        formatter.setMinimumFractionDigits(34)
        formatter.format(new java.math.BigDecimal(numberText))
          .replace("E", "E+")
      }
      case _ => numberText
    }
  }
  )


  def getValueBySection(section:Profile.Section, optionKey:String): String =
    section.get( optionKey, classOf[String])


  def log (base_int: Int, x: BigDecimal): java.math.BigDecimal ={

    case class Result (val value: BigDecimal = java.math.BigDecimal.ZERO) {
      def + (newValue: BigDecimal): Result = Result(this.value.add( newValue))

    }

    case class Input (val x: BigDecimal = new BigDecimal(x.toString())) {
      def apply(): BigDecimal = new BigDecimal(x.toString())
      def / (base_BigDecimal: BigDecimal, scale: Int, a_RoundingMode: RoundingMode):Input = {
        Input(x.divide(base_BigDecimal, scale, a_RoundingMode))
      }
      def * (newValue: BigDecimal): Input = Input(x.multiply(newValue))
    }


    val result = Result()
    val input= Input(x)
    val decimalPlaces = 100
    val scale: Int = input.x.precision + decimalPlaces
    val maxIte = 10
    val ite = 0
    val maxError_BigDecimal: BigDecimal = new BigDecimal(BigInteger.ONE,decimalPlaces + 1)
    val a_RoundingMode: RoundingMode = RoundingMode.UP
    val two_BigDecimal: BigDecimal = new BigDecimal("2")
    val base_BigDecimal: BigDecimal = new BigDecimal(base_int)

    @tailrec
    def updateResultandInput (input: Input, result: Result,i: Int = 1): (Input, Result) = {
      if (input.x.compareTo(base_BigDecimal) != i) (input, result)
      else {
        updateResultandInput(input./(base_BigDecimal, scale, a_RoundingMode), result.+(java.math.BigDecimal.ONE), i)
      }
    }

    val UpdateDone = updateResultandInput(input, result)
    val fraction: BigDecimal = new BigDecimal("0.5")
    val multiplyInput = UpdateDone._1.*(UpdateDone._1.x)
    val resultTemp = UpdateDone._2
    val resultPlusFraction = UpdateDone._2.+(fraction)


    @tailrec
    def finalLog(aResultPlusFraction: Result, aMultiplyInput: Input, aResult: Result, aFraction: BigDecimal, aIte: Int): Result ={
      if (aResultPlusFraction.value.compareTo(aResult.value) != 1
        && aMultiplyInput.x.compareTo(BigDecimal.ONE) != 1
      ) aResult
      else {
        val in = {
          if (aMultiplyInput.x.compareTo(base_BigDecimal) == 1) {
            val multTemp = aMultiplyInput./(base_BigDecimal, scale, a_RoundingMode)
            multTemp.*(multTemp.x)
          } else
            aMultiplyInput.*(aMultiplyInput.x)}
        val res = {
          if (aMultiplyInput.x.compareTo(base_BigDecimal) == 1)
            aResult.+(aFraction)
          else aResult
        }

        val frac = aFraction.divide(two_BigDecimal, scale,a_RoundingMode)
        val aResPF = res.+(frac)
        if ( frac.abs().compareTo(maxError_BigDecimal) == -1 || maxIte == aIte) res
        else
          finalLog(aResPF, in, res, frac, aIte+1)
      }
    }
    val finalRes = finalLog(resultPlusFraction, multiplyInput, resultTemp, fraction, ite)

    val aMathContext: MathContext = new MathContext(
      (decimalPlaces - 1) + (finalRes.value.precision - finalRes.value.scale),
      RoundingMode.HALF_UP
    )

    finalRes.value.round(aMathContext).stripTrailingZeros()
  }
  def oraSingleNumberRepresentationWithToNumber: UserDefinedFunction = udf((inputNumber: String) => {
    inputNumber.length match {
      case x if x > 40 => {
        inputNumber.substring(0,34) .+ {
          new java.math.BigDecimal(inputNumber.substring(34,35) + "." + inputNumber.substring(35)).setScale(0, java.math.RoundingMode.HALF_UP)
        }.padTo(inputNumber.length , '0')
      }
      case _ => inputNumber
    }
  }
  )
  def oraSingleNumberRepresentationWithoutToNumber: UserDefinedFunction = udf((inputNumber: String) => {
    inputNumber.length match {
      case x if x > 40 => {
        val i = if (inputNumber.length % 2 == 0) 39 else 38
        inputNumber.substring(0,i) .+ {
          new java.math.BigDecimal(inputNumber.substring(i,i+1) + "." + inputNumber.substring(i+1)).setScale(0, java.math.RoundingMode.HALF_UP)
        }.padTo(inputNumber.length , '0')
      }
      case _ => inputNumber
    }
  }
  )

  def oraNumberHash : UserDefinedFunction = udf((inputNumber: String) => {
    val dec = new java.math.BigDecimal(inputNumber)
    case class IntegerValues (val value: Int = 0) {
      def plusOne : IntegerValues = IntegerValues(this.value.+(1))
    }
    case class LMantissa (lMantStr: String) {
      def drop (lngth: Int) : LMantissa = LMantissa(this.lMantStr.drop(lngth))
      def dropRigth (lngth: Int) : LMantissa = LMantissa(this.lMantStr.dropRight(lngth))
    }
    case class LBytes (x: Array[Int] = Array()) {
      def push (value: Int) : LBytes = LBytes(this.x :+ value)
    }
    val l_abs_n = dec.abs()
    val l_exp = if (!l_abs_n.equals(BigDecimal.ZERO) )  log(100,l_abs_n).doubleValue().floor.toInt + 65 else 0
    val l_exp_proc  = if (dec >= BigDecimal.ZERO) l_exp + 128 else l_exp ^ 127
    val l_bytes = LBytes().push(l_exp_proc)
    val l_len = IntegerValues().plusOne
    val l_whole_part = l_abs_n.setScale(0, java.math.RoundingMode.DOWN).toString
    val l_whole_part_final = if (l_whole_part.length % 2 == 1)  '0' + l_whole_part else l_whole_part
    val l_frac_part = if(l_abs_n.remainder( BigDecimal.ONE ).toString.length>1)
      l_abs_n.remainder( BigDecimal.ONE ).toString.substring(2) else "";
    val l_frac_part_final = if (l_frac_part.length % 2 == 1) l_frac_part + '0' else l_frac_part
    val l_mantissa = LMantissa(l_whole_part_final + l_frac_part_final)

    @tailrec
    def dropLeadingZero (lmanista: LMantissa): LMantissa = {
      if (lmanista.lMantStr.take(2) != "00" ) lmanista
      else dropLeadingZero(lmanista.drop(2))
    }

    @tailrec
    def dropTailingZero (lmanista: LMantissa): LMantissa = {
      if (lmanista.lMantStr.takeRight(2) != "00" ) lmanista
      else dropTailingZero(lmanista.dropRigth(2))
    }

    val dropLeadingZeroLMantisaa = dropLeadingZero(l_mantissa)
    val dropTailingZeroLMantisaa = dropTailingZero(dropLeadingZeroLMantisaa)
    val MaxIte = math.floor(dropTailingZeroLMantisaa.lMantStr.length()/2).toInt.min(20)
    @tailrec
    def finalBytesRecur (lmanista: LMantissa, l_len: IntegerValues, l_bytes: LBytes, aIte: Int): (LBytes,IntegerValues) = {
      if (aIte == MaxIte ) (l_bytes, l_len)
      else {
        val l_chunk = lmanista.lMantStr.substring(aIte*2,aIte*2+2).toInt
        val l_chunk_neg = if (dec < BigDecimal.ZERO) 100-l_chunk else l_chunk
        finalBytesRecur(lmanista, l_len.plusOne, l_bytes.push(l_chunk_neg+1), aIte+1)
      }
    }
    val ite = 0
    val finalByteRec = finalBytesRecur(dropTailingZeroLMantisaa, l_len, l_bytes, ite)
    val finalByte = if (dec < BigDecimal.ZERO && finalByteRec._2.value < 21) finalByteRec._1.push(102).x else finalByteRec._1.x
    val xxx = finalByte.map(_.toByte)
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val finalHex = DatatypeConverter.printHexBinary(md.digest(xxx))
    finalHex
  }
  )



}
