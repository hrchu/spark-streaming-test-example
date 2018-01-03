package org.apache.spark.examples.streaming.integration;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.examples.streaming.JavaDirectKafkaWordCountTestable;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Ignore;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.IntStream;

/** Created by hrchu on 1/3/18. */
@Ignore("abstract")
public abstract class AbstractIT {
  private static final SparkConf SPARK_CONF =
      new SparkConf().setAppName("test").setMaster("local[2]");
  static int round;

  @Before
  public void setUp() throws Exception {
    Logger.getLogger("org").setLevel(Level.ERROR);
  }

  void runIt(int startRound, int endRound) throws InterruptedException {
    System.out.println("Round from " + startRound + " to " + endRound);
    JavaStreamingContext jssc = new JavaStreamingContext(SPARK_CONF, Durations.seconds(1));

    JavaRDD<String> inputRDD = jssc.sparkContext().textFile("in/log.txt");

    Queue<JavaRDD<String>> queue = new LinkedList<>();
    IntStream.rangeClosed(startRound, endRound).forEach(i -> queue.add(inputRDD));
    JavaDStream<String> lines = jssc.queueStream(queue);

    round = 0;

    JavaDStream<Tuple2<String, Integer>> result = JavaDirectKafkaWordCountTestable.parse(lines);

    result.foreachRDD(rdd -> round++);

    jssc.start();

    while (round < endRound) {
      jssc.awaitTerminationOrTimeout(1000);
    }

    jssc.stop();
  }

  void checkIt(Object answer) {
    // blah blah...
  }
}
