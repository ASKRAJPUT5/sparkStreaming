import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object streamingSpark {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder()
      .master("local")
      .appName("ASK Streaming Example")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") //*exception check

    val df = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()

    import spark.implicits._

    val wordsDF = df.select(explode(split(df("value")," ")).alias("word"))
    val count = wordsDF.groupBy("word").count()
    val query = count.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
//open a terminal and fire the command "nc -lk 9999" then start this code and then write stuffs on the terminal