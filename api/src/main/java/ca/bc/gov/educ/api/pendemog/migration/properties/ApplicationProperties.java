package ca.bc.gov.educ.api.pendemog.migration.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Class holds all application properties
 *
 * @author Marco Villeneuve
 */
@Component
@Getter
@Setter
public class ApplicationProperties {

  @Value("${threads.query}")
  private Integer queryThreads;
  @Value("${threads.executor}")
  private Integer executorThreads;
  @Value("${size.partition.entities.student.history}")
  private Integer partitionSize;
}
