package org.apache.flink.streaming.scala.examples.kafka

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

object Kafka010Example {

  def main(args: Array[String]): Unit = {

    // parse input arguments
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }

    val prefix = params.get("prefix", "PREFIX:")


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    env.enableCheckpointing(5000)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer = new FlinkKafkaConsumer010(
      params.getRequired("input-topic"),
      new SimpleStringSchema,
      params.getProperties)

    val messageStream = env
      .addSource(kafkaConsumer)
      .map(in => prefix + in)

    // create a Kafka producer for Kafka 0.10.x
    val kafkaProducer = new FlinkKafkaProducer010(
      params.getRequired("output-topic"),
      new SimpleStringSchema,
      params.getProperties)

    // write data into Kafka
    messageStream.addSink(kafkaProducer)

    env.execute("Kafka 0.10 Example")
  }

}
