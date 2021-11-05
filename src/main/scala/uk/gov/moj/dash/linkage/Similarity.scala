/** Simple Scala wrappers to turn an existing string similarity functions into
  * UDFs Additionaly some useful utilities are included
  */
package uk.gov.moj.dash.linkage

import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import org.apache.commons.text.similarity
import org.apache.commons.codec.language

class sqlEscape extends UDF1[String, String] {
  override def call(s: String): String = Literal(s).sql
}

object sqlEscape {
  def apply(): sqlEscape = {
    new sqlEscape()
  }
}

class DoubleMetaphone extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    val m = new language.DoubleMetaphone()
    m.doubleMetaphone(input)
  }
}

object DoubleMetaphone {
  def apply(): DoubleMetaphone = {
    new DoubleMetaphone()
  }
}

class BeiderMorseEncoder extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    val m = new language.bm.BeiderMorseEncoder()
    m.BeiderMorseEncoder.encode(input)
  }
}

object BeiderMorseEncoder {
  def apply(): BeiderMorseEncoder = {
    new BeiderMorseEncoder()
  }
}

class DoubleMetaphoneAlt extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    val m = new language.DoubleMetaphone()
    m.doubleMetaphone(input, true)
  }
}

object DoubleMetaphoneAlt {
  def apply(): DoubleMetaphoneAlt = {
    new DoubleMetaphoneAlt()
  }
}

class QgramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(2).toList.mkString(" ")

  }
}

object QgramTokeniser {
  def apply(): QgramTokeniser = {
    new QgramTokeniser()
  }
}

class Q2gramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(2).toList.mkString(" ")

  }
}

object Q2gramTokeniser {
  def apply(): Q2gramTokeniser = {
    new Q2gramTokeniser()
  }
}

class Q3gramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(3).toList.mkString(" ")

  }
}

object Q3gramTokeniser {
  def apply(): Q3gramTokeniser = {
    new Q3gramTokeniser()
  }
}

class Q4gramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(4).toList.mkString(" ")

  }
}

object Q4gramTokeniser {
  def apply(): Q4gramTokeniser = {
    new Q4gramTokeniser()
  }
}

class Q5gramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(5).toList.mkString(" ")

  }
}

object Q5gramTokeniser {
  def apply(): Q5gramTokeniser = {
    new Q5gramTokeniser()
  }
}

class Q6gramTokeniser extends UDF1[String, String] {
  override def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)

    input.sliding(6).toList.mkString(" ")

  }
}

object Q6gramTokeniser {
  def apply(): Q6gramTokeniser = {
    new Q6gramTokeniser()
  }
}

class JaroWinklerSimilarity extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    if ((left != null) & (right != null)) {
      val distance = new similarity.JaroWinklerDistance()
      distance(left, right)
    } else {
      0.0
    }

  }
}

object JaroWinklerSimilarity {
  def apply(): JaroWinklerSimilarity = {
    new JaroWinklerSimilarity()
  }
}

class JaccardSimilarity extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    if ((left != null) & (right != null)) {
      val distance = new similarity.JaccardSimilarity()
      distance(left, right)
    } else {
      0.0
    }

  }
}

object JaccardSimilarity {
  def apply(): JaccardSimilarity = {
    new JaccardSimilarity()
  }
}

class CosineDistance extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    if ((left != null) & (right != null)) {
      val distance = new similarity.CosineDistance()
      distance(left, right)
    } else {
      1.0
    }

  }
}

object CosineDistance {
  def apply(): CosineDistance = {
    new CosineDistance()
  }
}

class DualArrayExplode
    extends UDF2[Seq[String], Seq[String], Seq[(String, String)]] {
  override def call(x: Seq[String], y: Seq[String]): Seq[(String, String)] = {
    // This has to be instantiated here (i.e. on the worker node)

    val DualArrayExplode = (x: Seq[String], y: Seq[String]) => {

      if ((x != null) & (y != null)) {
        for (a <- x; b <- y) yield (a, b)
      } else { List() }

    }

    DualArrayExplode(x, y)

  }
}

object DualArrayExplode {
  def apply(): DualArrayExplode = {
    new DualArrayExplode()
  }
}

class latlongexplode extends UDF2[Seq[Row], Seq[Row], Seq[(Row, Row)]] {
  override def call(x: Seq[Row], y: Seq[Row]): Seq[(Row, Row)] = {

    val latlongexplode = (x: Seq[Row], y: Seq[Row]) => {

      if ((x != null) & (y != null)) {
        for (a <- x; b <- y) yield (a, b)
      } else { List() }
    }

    latlongexplode(x, y)

  }
}

object latlongexplode {
  def apply(): latlongexplode = {
    new latlongexplode()
  }
}
