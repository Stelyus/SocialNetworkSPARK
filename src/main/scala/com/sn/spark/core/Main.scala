import java.time.Instant

import com.sn.spark.Topic
import com.sn.spark.core.model._
import com.sn.spark.core.model.{Id, Message, Post, User}
import org.apache.kafka.clients.producer.KafkaProducer
import com.sn.spark.core.producer.{LikeProducer, LocationProducer, MessageProducer, PostProducer}
import com.sn.spark.core.consumer.{LikeConsumer, LocationConsumer, MessageConsumer, PostConsumer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._

import scala.util.Random

object Main extends App {
  override def main(args: Array[String]): Unit = {
  //  BasicConfigurator.configure()

//    sendPost()
//    sendMessage()
//    sendLike()
//    sendLocation()
//    val usr = new User("jean", "bernard", "jojo@gmail.com", "jojo", Instant.now(), false)
//    Cassandra.sendProfile(usr)
  }


  def sendMessage(): Unit = {
    val messageConsumer = MessageConsumer.createConsumer()
    val messageTopic: String = Topic.MessageToCassandra


    MessageConsumer.read(messageTopic, messageConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = messageConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
            val msg: Message = Message.deserialize(record.value())
            System.out.println("msg: " + msg.toString())
          }
        }
      }
    })


    val messageProducer: KafkaProducer[String, Array[Byte]] = MessageProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    val arrayUser: List[String] = List("Gabriel", "Adam", "Raphael", "Paul", "Louis", "Arthur", "Alexandre", "Victor",
      "Jules", "Mohamed", "Lucas", "Joseph", "Antoine", "Gaspard", "Maxime")
    val usersLength: Int = arrayUser.length


    val arrayText: List[String] = List("Amazing picture",
      "Sometimes you will never know the true value of a moment until it becomes a memory",
      "All might is the best !",
      "He is the worst friend ever",
      "Wanna eat Kimchi ?",
      "Want to go back to Korea ...")

    val textsLength = arrayText.length

    val r: Random = scala.util.Random


    while (i < 100) {
      MessageProducer.send[Message](
        messageTopic,
        Message(
          Id[Message]("message" + i),
          Instant.now(),
          Id[User](arrayUser(r.nextInt(usersLength))),
          Id[User](arrayUser(r.nextInt(usersLength))),
          arrayText(r.nextInt(textsLength)),
          deleted = false
        ),
        messageProducer
      )

      Thread.sleep(2000)
      i += 1
    }

    messageProducer.close()
  }

  def sendPost(): Unit = {
    val postConsumer = PostConsumer.createConsumer()
    val postTopic: String = Topic.PostsToCassandra

    PostConsumer.read(postTopic, postConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = postConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
            val msg: Message = Message.deserialize(record.value())
            System.out.println("msg: " + msg.toString())
          }
        }
      }
    })

    val postProducer: KafkaProducer[String, Array[Byte]] = PostProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    val arrayUser: List[String] = List("Gabriel", "Adam", "Raphael", "Paul", "Louis", "Arthur", "Alexandre", "Victor",
      "Jules", "Mohamed", "Lucas", "Joseph", "Antoine", "Gaspard", "Maxime")
    val usersLength: Int = arrayUser.length


    val arrayText: List[String] = List("Amazing picture",
      "Sometimes you will never know the true value of a moment until it becomes a memory",
      "All might is the best !",
      "He is the worst friend ever",
      "Wanna eat Kimchi ?",
      "Want to go back to Korea ...")

    val textsLength = arrayText.length

    val r: Random = scala.util.Random


    while (i < 100) {
      PostProducer.send[Post](
        postTopic,
        Post(
          Id[Post]("post"+i),
          Instant.now(),
          Id[User](arrayUser(r.nextInt(usersLength))),
          arrayText(r.nextInt(textsLength))
        ),
        postProducer
      )


      Thread.sleep(2000)
      i += 1
    }

    postProducer.close()
  }

  def sendLike(): Unit = {
    val likeConsumer = LikeConsumer.createConsumer()
    val likesTopic: String = Topic.LikeToCassandra

    LikeConsumer.read(likesTopic, likeConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = likeConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
            val msg: Message = Message.deserialize(record.value())
            System.out.println("msg: " + msg.toString())
          }
        }
      }
    })

    val likeProducer: KafkaProducer[String, Array[Byte]] = LikeProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    val arrayUser: List[String] = List("Gabriel", "Adam", "Raphael", "Paul", "Louis", "Arthur", "Alexandre", "Victor",
      "Jules", "Mohamed", "Lucas", "Joseph", "Antoine", "Gaspard", "Maxime")
    val usersLength: Int = arrayUser.length
    val r: Random = scala.util.Random

    while (i < 100) {
      LikeProducer.send[Like](
        likesTopic,
        Like(
          Id[Like]("like"+i),
          Id[User](arrayUser(r.nextInt(usersLength))),
          Instant.now(),
          Id[Post]("post"+i)
        ),
        likeProducer
      )


      Thread.sleep(2000)
      i += 1
    }

    likeProducer.close()
  }

  def sendLocation(): Unit = {
    val locConsumer = LocationConsumer.createConsumer()
    val locTopic: String = Topic.LocationToCassandra

    LocationConsumer.read(locTopic, locConsumer, new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = locConsumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
            val msg: Message = Message.deserialize(record.value())
            System.out.println("msg: " + msg.toString())
          }
        }
      }
    })

    val locProducer: KafkaProducer[String, Array[Byte]] = LocationProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic


    val arrayUser: List[String] = List("Gabriel", "Adam", "Raphael", "Paul", "Louis", "Arthur", "Alexandre", "Victor",
      "Jules", "Mohamed", "Lucas", "Joseph", "Antoine", "Gaspard", "Maxime")
    val usersLength: Int = arrayUser.length

    val cityCountryMap = Map(
      "Paris" -> "France",
      "Nice" -> "France",
      "Marseille" -> "France",
      "Tokyo" -> "Japon",
      "New York" -> "USA",
      "Saint-Barthelemy" -> "France",
      "Shanghai" -> "Chine",
      "Seoul" -> "Coree du Sud",
      "Berlin" -> "Allemagne",
      "Barcelone" -> "Espagne",
      "Madrid" -> "Espagne",
      "Taipei" -> "Taiwan",
      "Moscou" -> "Russie"
    )
    val cityMapSize = cityCountryMap.size

    val r: Random = scala.util.Random

    while (i < 100) {
      val city: String = cityCountryMap.keys.toSeq(r.nextInt(cityMapSize))
      val country: String = cityCountryMap.getOrElse(city, "Undefined")


      LocationProducer.send[Location](
        locTopic,
        Location(
          Id[Location]("location"+i),
          Instant.now(),
          Id[User](arrayUser(r.nextInt(usersLength))),
          city,
          country
        ),
        locProducer
      )


      Thread.sleep(2000)
      i += 1
    }

    locProducer.close()
  }

}