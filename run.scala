import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
//import com.databricks.spark.csv


/*val conf = new SparkConf().setAppName("Simpl App")
val sc =new org.apache.spark.SparkContext(conf)
val sqlc = new org.apache.spark.sql.SQLContext(sc)*/
//val rt = com.databricks.spark.csv()


val x = SQLContext.load("/Users/salonibindra/Documents/BIgD/Project/individual_security_exchange_2016_q4/dec16.csv") // uses implicit class CsvContext
x.printSchema()