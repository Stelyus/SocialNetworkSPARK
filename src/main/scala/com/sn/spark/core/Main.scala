import java.time.Instant

import com.sn.spark.core.model.{Id, Post, User}
import org.apache.kafka.clients.producer.KafkaProducer
import com.sn.spark.core.producer.PostProducer
import com.sn.spark.core.consumer.PostConsumer
import org.apache.log4j.BasicConfigurator

import scala.util.Random

object Main extends App {
  override def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val postConsumer = PostConsumer.createConsumer()
    val postTopic: String = "posts-topic"

    PostConsumer.read(postTopic, postConsumer)
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
          arrayText(r.nextInt(textsLength)) + " @" + arrayUser(r.nextInt(usersLength)),
          false
        ),
        postProducer
      )


      Thread.sleep(2000)
      i += 1
    }

    postProducer.close()
  }
}