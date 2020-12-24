package ca.bc.gov.educ.api.pendemog.migration;

import java.util.concurrent.atomic.AtomicInteger;

public final class CounterUtil {
  public static final AtomicInteger processCounter = new AtomicInteger();
  public static final AtomicInteger historyProcessCounter = new AtomicInteger();

  private CounterUtil() {

  }
}
