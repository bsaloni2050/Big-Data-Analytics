#Initializing PySpark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StringType
from pyspark import SQLContext

# #Spark Config
conf = SparkConf().setAppName("sample_app")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


rdd1 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/jan16.csv").map(lambda line: line.split(","))
rdd2 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/feb16.csv").map(lambda line: line.split(","))
rdd3 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/mar16.csv").map(lambda line: line.split(","))
rdd4 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/apr16.csv").map(lambda line: line.split(","))
rdd5 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/may16.csv").map(lambda line: line.split(","))
rdd6 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/jun16.csv").map(lambda line: line.split(","))
rdd7 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/jul16.csv").map(lambda line: line.split(","))
rdd8 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/aug16.csv").map(lambda line: line.split(","))
rdd9 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/sep16.csv").map(lambda line: line.split(","))
rdd10 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/oct16.csv").map(lambda line: line.split(","))
rdd11 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/nov16.csv").map(lambda line: line.split(","))
rdd12 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q1/dec16.csv").map(lambda line: line.split(","))

rdd_data = sc.union([rdd1, rdd2, rdd3,rdd4, rdd5, rdd6,rdd7, rdd8, rdd9,rdd10, rdd11, rdd12,])

data = rdd_data.toDF(['Date','Security','Ticker','Exchange','McapRank','TurnRank','VolatilityRank','PriceRank','Cancels','Trades',
'LitTrades','OddLots','Hidden','TradesForHidden','OrderVol','TradeVol','LitVol','OddLotVol','HiddenVol','TradeVolForHidden'])

data.registerTempTable("data")

trades_avg=sqlContext.sql("SELECT avg(Trades),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")
trades_avg.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/Outputs/Trades_Output')

cancels_avg=sqlContext.sql("SELECT avg(Trades),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")
cancels_avg.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/Outputs/Cancels_Output')

littrades_avg=sqlContext.sql("SELECT avg(Trades),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")
littrades_avg.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/Outputs/LitTrades_Output')


oddlots_avg=sqlContext.sql("SELECT avg(Trades),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")
oddlots_avg.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/Outputs/Oddlots_Output')

Hidden_avg=sqlContext.sql("SELECT avg(Trades),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")
Hidden_avg.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/Outputs/Hidden_Output')

