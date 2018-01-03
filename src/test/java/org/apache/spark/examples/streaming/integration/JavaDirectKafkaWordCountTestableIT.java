package org.apache.spark.examples.streaming.integration;

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/** Created by hrchu on 1/3/18. */
public class JavaDirectKafkaWordCountTestableIT extends AbstractIT {
  @Test
  public void test() throws Exception {
    Set<Object> answer;

    runIt(1, 1);

    answer = Collections.emptySet();
    checkIt(answer);

    runIt(2, 5);

    answer = Collections.emptySet();
    checkIt(answer);
  }
}
