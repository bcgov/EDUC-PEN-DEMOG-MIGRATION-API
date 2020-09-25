package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentTwinEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentRepository;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
public class StudentPersistenceService {
  private final StudentRepository studentRepository;

  @Autowired
  public StudentPersistenceService(StudentRepository studentRepository) {
    this.studentRepository = studentRepository;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 200, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveStudents(List<StudentEntity> studentEntities) {
    if (studentEntities.size() > 1000) {
      List<List<StudentEntity>> subSets = Lists.partition(studentEntities, 1000);
      log.info("created subset of {} student entities", subSets.size());
      subSets.forEach(studentRepository::saveAll);
    } else {
      studentRepository.saveAll(studentEntities);
    }
  }
}
