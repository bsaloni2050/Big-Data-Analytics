import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions



val Org_data = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/dec162.csv").map(line => line.split(","))

case class cc1(Date:String,	Security:String,	Ticker:String,	Exchange:String,	Cancels:Int,	Trades:Int,	OddLots:Int,	Hidden:Int)

val data1 = Org_data.map( p => cc1( p(0), p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,p(7).toInt))

val data = data1.toDF()



val header = data.first
val rows = data.filter(l => l != header)
rows.printSchema
// rows.take(2)
// Date	Security	Ticker	Exchange	McapRank	TurnRank	VolatilityRank	PriceRank	Cancels	Trades	LitTrades	OddLots	Hidden	TradesForHidden	OrderVol('000)	TradeVol('000)	LitVol('000)	OddLotVol('000)	HiddenVol('000)	TradeVolForHidden('000)

// // define case class
// case class CC1(lpep_pickup_datetime: Double,	Lpep_dropoff_datetime: Double,	Pickup_longitude: Double,	Pickup_latitude: Double,	Dropoff_longitude: Double,	Dropoff_latitude: Double,	Passenger_count: Double,	Trip_distance: Double,	Fare_amount: Double,	Extra: Double,	Tip_amount: Double,	Tolls_amount: Double,	Total_amount: Double,	Payment_type: Double,	Trip_type : Double)

// // comma separator split
// val allSplit = rows.map(line => line.split(","))

// // map parts to case class
// val allData = allSplit.map( p => CC1( p(0).toDouble, p(1).toDouble, p(2).trim.toDouble, p(3).trim.toDouble, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim.toDouble, p(9).trim.toDouble,p(10).trim.toDouble,p(11).trim.toDouble,p(12).trim.toDouble))

// // convert rdd to dataframe
// val allDF = allData.toDF()

// // convert back to rdd and cache the data
// val rowsRDD = allDF.rdd.map(r => (r.getString(0), r.getString(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9) ,p(10).trim.toDouble,p(11).trim.toDouble,p(12).trim.toDouble))

// rowsRDD.cache()

// // convert data to RDD which will be passed to KMeans and cache the data. Assign attributes to cluster from 0-4.

// val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4) ))

// vectors.cache()

// //KMeans model with 2 clusters and 20 iterations
// val kMeansModel = KMeans.train(vectors, 2, 20)

// //Print the center of each cluster
// kMeansModel.clusterCenters.foreach(println)
