package ca.bc.gov.educ.api.pendemog.migration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.retry.annotation.EnableRetry;

/**
 * The type Pen demographics api resource application.
 */
@SpringBootApplication
@EnableCaching
@EnableRetry
public class PenDemographicsDataMigrationApiResourceApplication {

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(PenDemographicsDataMigrationApiResourceApplication.class, args);
  }

}
