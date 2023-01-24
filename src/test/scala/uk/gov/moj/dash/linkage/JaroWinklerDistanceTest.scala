package uk.gov.moj.dash.linkage

import org.scalactic.TolerantNumerics
import org.scalatest._

class JaroWinklerSimilarityTest extends FlatSpec with Matchers {

    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

    "Wrapped JaroWinkler" should "return same results" in {

        val sim = JaroWinklerSimilarity()
        sim.call("MARHTA", "MARTHA") should equal (0.96)
        sim.call("nope", "yes") should equal (0.0)


    }
}


