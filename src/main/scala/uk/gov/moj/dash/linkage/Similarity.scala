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
import scala.collection.mutable
import scala.math._
import breeze.linalg.{DenseVector, normalize}


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
      val jwsim = new similarity.JaroWinklerSimilarity()
      jwsim(left, right)
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


class LevDamerauDistance extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {


    if ((left != null) & (right != null)) {

  val len1 = left.length
  val len2 = right.length
  val infinite = len1 + len2

  // character array
  val da = mutable.Map[Char, Int]().withDefaultValue(0)

  // distance matrix
  val score = Array.fill(len1 + 2, len2 + 2)(0)

  score(0)(0) = infinite
  for (i <- 0 to len1) {
    score(i + 1)(0) = infinite
    score(i + 1)(1) = i
  }
  for (i <- 0 to len2) {
    score(0)(i + 1) = infinite
    score(1)(i + 1) = i
  }

  for (i <- 1 to len1) {
    var db = 0
    for (j <- 1 to len2) {
      val i1 = da(right(j - 1))
      val j1 = db
      var cost = 1
      if (left(i - 1) == right(j - 1)) {
        cost = 0
        db = j
      }

      score(i + 1)(j + 1) = List(
        score(i)(j) + cost,
        score(i + 1)(j) + 1,
        score(i)(j + 1) + 1,
        score(i1)(j1) + (i - i1 - 1) + 1 + (j - j1 - 1)
      ).min
    }
    da(left(i - 1)) = i
  }

  score(len1 + 1)(len2 + 1)


    } else {
      100.0
    }
  }
}

object LevDamerauDistance {
  def apply(): LevDamerauDistance = {
    new LevDamerauDistance()
  }
}



class JaroSimilarity extends UDF2[String, String, Double] {
  override def call(left: String, right: String): Double = {
    

    if ((left != null) & (right != null)) {

       val s1_len = left.length
        val s2_len = right.length
        if (s1_len == 0 && s2_len == 0) return 1.0
        val match_distance = Math.max(s1_len, s2_len) / 2 - 1
        val s1_matches = Array.ofDim[Boolean](s1_len)
        val s2_matches = Array.ofDim[Boolean](s2_len)
        var matches = 0
        for (i <- 0 until s1_len) {
            val start = Math.max(0, i - match_distance)
            val end = Math.min(i + match_distance + 1, s2_len)
            start until end find { j => !s2_matches(j) && left(i) == right(j) } match {
                case Some(j) =>
                    s1_matches(i) = true
                    s2_matches(j) = true
                    matches += 1
                case None =>
            }
        }
        if (matches == 0) return 0.0
        var t = 0.0
        var k = 0
        0 until s1_len filter s1_matches foreach { i =>
            while (!s2_matches(k)) k += 1
            if (left(i) != right(k)) t += 0.5
            k += 1
        }

        val m = matches.toDouble
        
        (m / s1_len + m / s2_len + (m - t) / m) / 3.0


    } else {
      0.0
    }
  }
}

object JaroSimilarity {
  def apply(): JaroSimilarity = {
    new JaroSimilarity()
  }
}

class VectorCosineSimilarity extends UDF2[Seq[Double], Seq[Double], Double] {
  // Step 3: Override the `call` method to compute cosine similarity between two embeddings
  override def call(embedding1: Seq[Double], embedding2: Seq[Double]): Double = {
    val vec1 = DenseVector(embedding1.toArray)
    val vec2 = DenseVector(embedding2.toArray)

    // Normalize the input vectors and compute the dot product
    val cosineSimilarity = normalize(vec1).dot(normalize(vec2))

    cosineSimilarity
  }
}

object VectorCosineSimilarity {
  def apply(): VectorCosineSimilarity = {
    new VectorCosineSimilarity()
  }
}
