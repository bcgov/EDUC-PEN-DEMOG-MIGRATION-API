package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.GradeCodes;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenAuditStudentHistoryMapper;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenDemogStudentMapper;
import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@Slf4j
public class StudentService {
  private final Set<String> gradeCodes = new HashSet<>();
  private static final PenDemogStudentMapper studentMapper = PenDemogStudentMapper.mapper;
  private static final PenAuditStudentHistoryMapper PEN_AUDIT_STUDENT_HISTORY_MAPPER = PenAuditStudentHistoryMapper.mapper;
  private final StudentPersistenceService studentPersistenceService;

  @Autowired
  public StudentService(StudentPersistenceService studentPersistenceService) {
    this.studentPersistenceService = studentPersistenceService;
  }

  @PostConstruct
  public void init() {
    gradeCodes.addAll(Arrays.stream(GradeCodes.values()).map(GradeCodes::getCode).collect(Collectors.toSet()));
    log.info("Added all the grade codes {}", gradeCodes.size());
  }

  public boolean processDemographicsEntities(List<PenDemographicsEntity> penDemographicsEntities, String studNoLike) {
    var currentLotSize = penDemographicsEntities.size();
    if (currentLotSize > 15000) {
      var chunks = Lists.partition(penDemographicsEntities, 10000);
      for (var chunk : chunks) {
        processChunk(chunk, studNoLike, currentLotSize);
      }
    } else {
      processChunk(penDemographicsEntities, studNoLike, currentLotSize);
    }


    log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
    return true;
  }

  private void processChunk(List<PenDemographicsEntity> penDemographicsEntities, String studNoLike, int currentLotSize) {
    List<StudentEntity> studentEntities = new ArrayList<>();
    var index = 1;
    for (var penDemog : penDemographicsEntities) {
      if (penDemog.getStudNo() != null && penDemog.getStudBirth() != null) {
        log.debug("Total Records :: {} , processing pen :: {} at index {}, for studNoLike {}", currentLotSize, penDemog.getStudNo(), index, studNoLike);
        penDemog.setStudBirth(getFormattedDOB(penDemog.getStudBirth())); //update the format
        var mappedStudentRecord = studentMapper.toStudent(penDemog);
        try {
          LocalDate dob = LocalDate.parse(penDemog.getStudBirth());
          mappedStudentRecord.setDob(dob);
        } catch (final Exception e) {
          mappedStudentRecord.setDob(LocalDate.parse("2000-01-01"));
        }
        if (mappedStudentRecord.getGradeCode() != null && !gradeCodes.contains(mappedStudentRecord.getGradeCode().trim().toUpperCase())) {
          log.debug("updated grade code to null from :: {} at index {}, for studNoLike {}", mappedStudentRecord.getGradeCode(), index, studNoLike);
          mappedStudentRecord.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
        }
        if (mappedStudentRecord.getLegalLastName() == null || mappedStudentRecord.getLegalLastName().trim().equals("")) {
          mappedStudentRecord.setLegalLastName("NULL");
        }
        if(StringUtils.isBlank(mappedStudentRecord.getCreateUser())){
          log.debug("updating create user from null or blank");
          mappedStudentRecord.setCreateUser("PEN_MIGRATION_API");
        }
        if(StringUtils.isBlank(mappedStudentRecord.getUpdateUser())){
          log.debug("updating update user from null or blank");
          mappedStudentRecord.setUpdateUser("PEN_MIGRATION_API");
        }
        studentEntities.add(mappedStudentRecord);
      } else {
        log.error("NO PEN AND STUD BIRTH skipping this record at index {}, for studNoLike {}", index, studNoLike);
      }
      index++;
    }
    if (!studentEntities.isEmpty()) {
      try {
        studentPersistenceService.saveStudents(studentEntities);
        log.debug("processing complete for studNoLike :: {}, persisted {} records into DB", studNoLike, studentEntities.size());
      } catch (final Exception ex) {
        log.error("Exception while persisting records for studNoLike :: {}, records into DB , exception is :: {}", studNoLike, ex);
        throw ex;
      }
    }
  }


  public List<StudentHistoryEntity> processDemographicsAuditEntities(List<PenAuditEntity> penAuditEntities, Map<String, UUID> penStudIDMap) {
    var currentLotSize = penAuditEntities.size();
    List<StudentHistoryEntity> studentHistoryEntities = new ArrayList<>();
    final AtomicInteger recordCount = new AtomicInteger(0);
    for (var penAuditEntity : penAuditEntities) {
      recordCount.incrementAndGet();

      if (penAuditEntity != null && penAuditEntity.getPen() != null) {
        log.debug("Total Records :: {} , processing audit entity :: {} at index {}, for pen {}", currentLotSize, penAuditEntity, recordCount.get(), penAuditEntity.getPen());
        try {
          var studentHistory = PEN_AUDIT_STUDENT_HISTORY_MAPPER.toStudentHistory(penAuditEntity);
          studentHistory.setStudentID(penStudIDMap.get(studentHistory.getPen()));
          if (studentHistory.getGradeCode() != null && !gradeCodes.contains(studentHistory.getGradeCode().trim().toUpperCase())) {
            log.debug("updated grade code to null from :: {} at index {}, for pen {}", studentHistory.getGradeCode(), recordCount.get(), penAuditEntity.getPen());
            studentHistory.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
          }
          if (StringUtils.isBlank(studentHistory.getLegalLastName())) {
            studentHistory.setLegalLastName("NULL");
          }
          studentHistoryEntities.add(studentHistory);
        } catch (final Exception ex) {
          log.error("exception while processing entity :: {}, {}", penAuditEntity, ex);
        }
      } else {
        log.info("pen audit entity at {} is :: {}", recordCount.get(), penAuditEntity != null ? penAuditEntity.toString() : "");
      }
    }
    return studentHistoryEntities;
  }


  private String getFormattedDOB(String dob) {
    var year = dob.substring(0, 4);
    var month = dob.substring(4, 6);
    var day = dob.substring(6, 8);

    return year.concat("-").concat(month).concat("-").concat(day);
  }

  public void saveHistoryEntities(List<StudentHistoryEntity> historyEntitiesToPersist) {
    studentPersistenceService.saveStudentHistory(historyEntitiesToPersist);
  }
}
