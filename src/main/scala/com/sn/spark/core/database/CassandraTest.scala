import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark._
import com.sn.spark.core.model.Message

object CassandraTest {
  val conf = new SparkConf(true).setAppName("Cassandra").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)

  def sendMessage(msg : Message) : Unit ={
    val rdd = sc.cassandraTable("spark", "mesage")

  }
}
