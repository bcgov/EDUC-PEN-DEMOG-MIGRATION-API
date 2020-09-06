package ca.bc.gov.educ.api.pendemog.migration;

import java.util.concurrent.atomic.AtomicInteger;

public final class CounterUtil {
  public static final AtomicInteger processCounter = new AtomicInteger();

  private CounterUtil() {

  }
}
