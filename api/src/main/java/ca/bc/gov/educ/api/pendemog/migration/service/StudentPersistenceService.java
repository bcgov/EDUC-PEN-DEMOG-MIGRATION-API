package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentMergeEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentHistoryRepository;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentMergeRepository;
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
  private final StudentHistoryRepository studentHistoryRepository;
  private final StudentMergeRepository studentMergeRepository;

  @Autowired
  public StudentPersistenceService(final StudentRepository studentRepository, final StudentHistoryRepository studentHistoryRepository, final StudentMergeRepository studentMergeRepository) {
    this.studentRepository = studentRepository;
    this.studentHistoryRepository = studentHistoryRepository;
    this.studentMergeRepository = studentMergeRepository;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 20, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveStudents(final List<StudentEntity> studentEntities) {
    if (studentEntities.size() > 1000) {
      final List<List<StudentEntity>> subSets = Lists.partition(studentEntities, 1000);
      subSets.forEach(this.studentRepository::saveAll);
    } else {
      this.studentRepository.saveAll(studentEntities);
    }
  }

  public StudentEntity getStudentByPen(final String pen) {
    return this.studentRepository.getByPen(pen);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 20, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveStudentHistory(final List<StudentHistoryEntity> studentHistoryEntities) {
    if (studentHistoryEntities.size() > 1500) {
      final List<List<StudentHistoryEntity>> subSets = Lists.partition(studentHistoryEntities, 1000);
      subSets.forEach(this.studentHistoryRepository::saveAll);
    } else {
      this.studentHistoryRepository.saveAll(studentHistoryEntities);
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 20, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveMergesAndStudentUpdates(final List<StudentMergeEntity> mergeFromEntities, final List<StudentMergeEntity> mergeTOEntities, final List<StudentEntity> mergedStudents) {
    final List<List<StudentMergeEntity>> mergeFromSubset = Lists.partition(mergeFromEntities, 1000);
    final List<List<StudentMergeEntity>> mergeToSubset = Lists.partition(mergeTOEntities, 1000);
    final List<List<StudentEntity>> mergedStudentsSubset = Lists.partition(mergedStudents, 1000);
    log.info("created subset of {} merge from entities", mergeFromSubset.size());
    log.info("created subset of {} merge to entities", mergeToSubset.size());
    log.info("created subset of {} student entities", mergedStudentsSubset.size());
    mergeFromSubset.forEach(this.studentMergeRepository::saveAll);
    mergeToSubset.forEach(this.studentMergeRepository::saveAll);
    mergedStudentsSubset.forEach(this.studentRepository::saveAll);
  }

  @Retryable(value = {Exception.class}, maxAttempts = 20, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void updateStudentWithMemos(final List<StudentEntity> memoStudents) {
    final List<List<StudentEntity>> memoStudentsSubset = Lists.partition(memoStudents, 1000);
    memoStudentsSubset.forEach(this.studentRepository::saveAll);
  }
}
