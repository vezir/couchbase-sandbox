import com.couchbase.spark.kv.{KeyValueOptions, Upsert}
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Quickstart {

  case class Airline(name: String, country: String)
  case class Airline3(name: String, iata: String, icao: String, country: String)
  def main(args: Array[String]): Unit = {

    // Configure the Spark Session
    val spark = SparkSession
      .builder()
      .appName("Couchbase Quickstart")
      //.master("local") // use the JVM as the master, great for testing
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "user")
      .config("spark.couchbase.password", "123456")
      //.config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()

    import com.couchbase.spark._

    // Imports needed, but let your IDE handle that!
    import com.couchbase.client.scala.json.JsonObject
    import com.couchbase.spark.kv.Get

    // Create a DataFrame with Schema Inference
    val airlines = spark.read
      .format("couchbase.query")
      //.format("couchbase.analytics")
      //.option(QueryOptions.Filter, "type = 'airline'")
      .option(QueryOptions.Bucket, "travel-sample")
      .option(QueryOptions.Scope, "inventory")
      .option(QueryOptions.Collection, "airline")
      .load()

    // Print The Schema
    airlines.printSchema()

    import spark.sqlContext.implicits._

    airlines
      .select("name", "callsign")
      .sort(airlines("name").asc)
      .show(10)

    // Create a Dataset from the DataFrame
    val airlinesDS = airlines.as[Airline]
    airlinesDS
      .limit(10)
      .collect()
      .foreach(println)

    val ks = Keyspace(
      bucket = Option("travel-sample"),
      scope = Option("inventory"),
      collection = Option("airline")
    )
    spark
      .sparkContext
      .couchbaseGet(Seq(Get("airline_10"), Get("airline_10642")), keyspace = ks)
      .collect()
      .foreach(result => println(result.contentAs[JsonObject]))

    //Spark SQL DataFrames
    val airlines2 = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, "travel-sample")
      .option(QueryOptions.Scope, "inventory")
      .option(QueryOptions.Collection, "airline")
      .schema(StructType(
        StructField("name", StringType) ::
          StructField("type", StringType) :: Nil
      ))
      .load()
    airlines2.show()

    airlines2
      .select("name", "type")
      .sort(airlines2("name").desc)
      .show(10)

    //DataFrame persistence
    //You can store DataFrames using both couchbase.query and couchbase.kv.
    // We recommend using the KeyValue data source since it provides better
    // performance out of the box if the usage pattern allows for it.

    val airlines3 = spark.read.format("couchbase.query")
      .option(QueryOptions.Bucket, "travel-sample")
      .option(QueryOptions.Scope, "inventory")
      .option(QueryOptions.Collection, "airline")
      //.option(QueryOptions.Filter, "type = 'airline'")
      .load()
      .limit(50000)
    airlines3.show()
    //import spark.implicits._
    airlines3.write.mode(SaveMode.Overwrite).format("couchbase.kv")
      .option(KeyValueOptions.Bucket, "test")
      .option(KeyValueOptions.StreamFrom, KeyValueOptions.StreamFromBeginning)
      //.option(KeyValueOptions.Durability, KeyValueOptions.MajorityDurability)
      .save()

    //Working with Datasets
    val airlines4 = spark.read.format("couchbase.query")
      .option(KeyValueOptions.Bucket, "test")
      .option(QueryOptions.Filter, "type = 'airline'")
      .load()
      .as[Airline3]
    airlines4
      .map(_.name)
      .filter(_.toLowerCase.startsWith("a"))
      .foreach(println(_))

    println("SparkSession.active.sparkContext.defaultParallelism=", SparkSession.active.sparkContext.defaultParallelism)
  }

}