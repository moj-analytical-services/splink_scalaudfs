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


class JaroSimilarity extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)


def jaroSimilarityinScala(s1: String, s2: String): Double = {
  // Compute the length of the strings
  val m = s1.length
  val n = s2.length

  // Initialize the number of matching characters to 0
  var matching_characters = 0

  // Create a flag array to track which characters have been used
  val used = Array.fill(m + n)(false)

  // Iterate over the characters in s1 and s2
  for (i <- 0 until m; j <- 0 until n) {
    // Check if the characters are equal and within a certain distance from each other
    if (s1(i) == s2(j) && !used(i) && !used(j) && math.abs(i - j) <= (m - n).max(n - m)) {
      // If the characters are equal and within the allowed distance, increment the number of matching characters and mark them as used
      matching_characters += 1
      used(i) = true
      used(j) = true
    }
  }

  // Compute the Jaro similarity as the number of matching characters divided by the average length of the strings
  matching_characters.toDouble / (m + n) / 2
}

    if ((left != null) & (right != null)) {
      jaroSimilarityinScala(left, right)
    } else {
      0.0
    }

  }}

  object JaroSimilarity {
  def apply(): JaroSimilarity = {
    new JaroSimilarity()
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
