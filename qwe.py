#Initializing PySpark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StringType
from pyspark import SQLContext

# #Spark Config
conf = SparkConf().setAppName("sample_app")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


rdd1 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/dec16.csv").map(lambda line: line.split(","))
rdd2= sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/nov16.csv").map(lambda line: line.split(","))
rdd3 = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/oct16.csv").map(lambda line: line.split(","))

rdd_data = sc.union([rdd1, rdd2, rdd3])

#rdd_data = sc.textFile("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/dec16.csv").map(lambda line: line.split(","))




data = rdd_data.toDF(['Date','Security','Ticker','Exchange','McapRank','TurnRank','VolatilityRank','PriceRank','Cancels','Trades',
'LitTrades','OddLots','Hidden','TradesForHidden','OrderVol','TradeVol','LitVol','OddLotVol','HiddenVol','TradeVolForHidden'])

#data.show()
data.registerTempTable("data")

# exchanges=sqlContext.sql("SELECT DISTINCT Exchange FROM data")
# exchanges.show()

cancels=sqlContext.sql("SELECT avg(Cancels),Ticker,Exchange FROM data group by Exchange,Ticker order by Ticker asc")


#cancels=sqlContext.sql("SELECT avg(Cancels),Ticker,Exchange FROM data group by Exchange,Ticker")
cancels.show()

cancels.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/out')

# exchanges.write.text
# results = exchanges.collect()
# results.rdd.saveAsTextFile("/Users/salonibindra/Documents/BIgD/Project/myoutput.txt")

