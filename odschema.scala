package edu.vcu.sparkminiproject

case class odSchema(fips: String, year: Int, drugType: String, rateVisits: Float, visitCount: Int)

case class acsSchema(geoId: String, name: String, totalFamilies: Int, numBelowPoverty: Int, fips: String)
