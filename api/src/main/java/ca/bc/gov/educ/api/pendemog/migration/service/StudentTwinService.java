package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentTwinEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentTwinRepository;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
public class StudentTwinService {

  @Getter(AccessLevel.PRIVATE)
  private final StudentTwinRepository studentTwinRepository;

  @Autowired
  public StudentTwinService(StudentTwinRepository studentTwinRepository) {
    this.studentTwinRepository = studentTwinRepository;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 10, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveTwinnedEntities(List<StudentTwinEntity> twinEntities){
    if (twinEntities.size() > 5000) {
      List<List<StudentTwinEntity>> subSets = Lists.partition(twinEntities, 5000);
      log.info("created subset of {} twinned entities", subSets.size());
      subSets.forEach(studentTwinEntities -> getStudentTwinRepository().saveAll(studentTwinEntities));
    } else {
      getStudentTwinRepository().saveAll(twinEntities);
    }
  }
}
