/**
  * Simple Scala wrappers to turn an existing string similarity functions into UDFs
  */
package uk.gov.moj.dash.linkage

import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF1


import org.apache.commons.text.similarity
import org.apache.commons.codec.language


import scala.math.log
import scala.math.exp
 
class Logit extends UDF1[Double, Double] {
  override def call(x: Double): Double =  (log(x / (1.0 - x))).toDouble
}


object Logit {
  def apply(): Logit = {
    new Logit()
  }
}



class Expit extends UDF1[Double, Double] {
  override def call(x: Double): Double =  (1.0 / (1.0 + exp(-x))).toDouble
}


object Expit {
  def apply(): Expit = {
    new Expit()
  }
}


class DoubleMetaphone extends UDF1[String, String] {
  override  def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)
   

    val  m = new language.DoubleMetaphone()
    m.doubleMetaphone(input)
  }
}

object DoubleMetaphone {
  def apply(): DoubleMetaphone = {
    new DoubleMetaphone()
  }
}



class DoubleMetaphoneAlt extends UDF1[String, String] {
  override  def call(input: String): String = {
    // This has to be instantiated here (i.e. on the worker node)
   

    val  m = new language.DoubleMetaphone()
    m.doubleMetaphone(input,true)
  }
}

object DoubleMetaphoneAlt {
  def apply(): DoubleMetaphoneAlt = {
    new DoubleMetaphoneAlt()
  }
}



class QgramTokeniser extends UDF1[String, String] {
  override  def call(input: String): String = {
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
  override  def call(input: String): String = {
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
  override  def call(input: String): String = {
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
  override  def call(input: String): String = {
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
  override  def call(input: String): String = {
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
  override  def call(input: String): String = {
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
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.JaroWinklerDistance()
    distance(left, right)
  }
}

object JaroWinklerSimilarity {
  def apply(): JaroWinklerSimilarity = {
    new JaroWinklerSimilarity()
  }
}


class JaccardSimilarity extends UDF2[String, String, Double] {
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.JaccardSimilarity()
    distance(left, right)
  }
}

object JaccardSimilarity {
  def apply(): JaccardSimilarity = {
    new JaccardSimilarity()
  }
}



class CosineDistance extends UDF2[String, String, Double] {
  override  def call(left: String, right: String): Double = {
    // This has to be instantiated here (i.e. on the worker node)
    val distance = new similarity.CosineDistance()
    distance(left, right)
  }
}

object CosineDistance {
  def apply(): CosineDistance = {
    new CosineDistance()
  }
}



class DualArrayExplode extends UDF2[Seq[String], Seq[String],  Seq[(String,String)]] {
  override  def call(x: Seq[String], y: Seq[String]): Seq[(String,String)] = {
    // This has to be instantiated here (i.e. on the worker node)
    
  val DualArrayExplode =  (x: Seq[String], y: Seq[String]) => {
    
    
        if ((x != null) & (y != null)){
    for (a <- x; b <-y) yield (a,b)
     } else
    {List()}
    
    }

  DualArrayExplode(x,y)
 

  }
}

object DualArrayExplode {
  def apply(): DualArrayExplode = {
    new DualArrayExplode()
  }
}
