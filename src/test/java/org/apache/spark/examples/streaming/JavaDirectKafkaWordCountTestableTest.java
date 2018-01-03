package org.apache.spark.examples.streaming;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by hrchu on 1/3/18. */
public class JavaDirectKafkaWordCountTestableTest extends AbstractTest {
  @Test
  public void wordCount() throws Exception {
    JavaDStream<String> lines =
        createInput(
            Arrays.asList(
                "Jan 3 12:54:06 localhost nfcapd[1231]: Ident: 'none' Flows: 0, Packets: 0, Bytes: 0, Sequence Errors: 0, Bad Packets: 0",
                "Jan 3 13:19:06 localhost nfcapd[1231]: Ident: 'none' Flows: 0, Packets: 0, Bytes: 0, Sequence Errors: 0, Bad Packets: 0"));

    // expected output
    Set<Tuple2<String, Integer>> expected =
        Arrays.asList(
                new Tuple2<>("0,", 8),
                new Tuple2<>("12:54:06", 1),
                new Tuple2<>("0", 2),
                new Tuple2<>("Ident:", 2),
                new Tuple2<>("13:19:06", 1),
                new Tuple2<>("'none'", 2),
                new Tuple2<>("Bytes:", 2),
                new Tuple2<>("Packets:", 4),
                new Tuple2<>("Jan", 2),
                new Tuple2<>("Bad", 2),
                new Tuple2<>("nfcapd[1231]:", 2),
                new Tuple2<>("Errors:", 2),
                new Tuple2<>("3", 2),
                new Tuple2<>("Sequence", 2),
                new Tuple2<>("Flows:", 2),
                new Tuple2<>("localhost", 2))
            .stream()
            .collect(Collectors.toSet());

    JavaDStream<Tuple2<String, Integer>> result = JavaDirectKafkaWordCountTestable.wordCount(lines);

    runAndCheck(expected, result);
  }
}
