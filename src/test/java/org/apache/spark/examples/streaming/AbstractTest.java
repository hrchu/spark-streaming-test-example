package org.apache.spark.examples.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Ignore;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;

/** Created by hrchu on 1/3/18. */
@Ignore("abstract")
public class AbstractTest {
  private static final SparkConf SPARK_CONF =
      new SparkConf().setAppName("test").setMaster("local[2]");
  private static JavaStreamingContext jssc;
  private static boolean done;

  static JavaDStream<String> createInput(List<String> log) {
    JavaRDD<String> rdd = jssc.sparkContext().parallelize(log);
    Queue<JavaRDD<String>> queue = new LinkedList<>();
    queue.add(rdd);
    return jssc.queueStream(queue);
  }

  static void runAndCheck(Set expected, JavaDStream<?> result) throws InterruptedException {
    result.foreachRDD(
        rdd -> {
          rdd.foreach(r -> assertTrue(expected.stream().anyMatch(t -> t.equals(r))));

          done = true;
        });

    jssc.start();

    do {
      jssc.awaitTerminationOrTimeout(1000);
    } while (!done);

    jssc.stop();
  }

  @Before
  public void setUp() throws Exception {
    Logger.getLogger("org").setLevel(Level.ERROR);

    jssc = new JavaStreamingContext(SPARK_CONF, Durations.seconds(1));

    done = false;
  }
}
