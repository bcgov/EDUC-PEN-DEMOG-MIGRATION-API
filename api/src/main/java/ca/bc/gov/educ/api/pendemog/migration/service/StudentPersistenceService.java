package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentRepository;
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
    studentRepository.saveAll(studentEntities);
  }
}
