package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.GradeCodes;
import ca.bc.gov.educ.api.pendemog.migration.constants.HistoryActivityCode;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenAuditStudentHistoryMapper;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenDemogStudentMapper;
import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentRepository;
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
import java.time.LocalDateTime;
import java.util.*;
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

  @Retryable(value = {Exception.class}, backoff = @Backoff(multiplier = 2, delay = 2000))
  public boolean processDemographicsEntities(List<PenDemographicsEntity> penDemographicsEntities, String studNoLike) {
    var currentLotSize = penDemographicsEntities.size();
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
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<StudentEntity>> violations = validator.validate(mappedStudentRecord);
        for (var violation : violations) {
          if ("Invalid postal code".equalsIgnoreCase(violation.getMessage())) {
            mappedStudentRecord.setPostalCode(null);
          }
        }
        if (mappedStudentRecord.getGradeCode() != null && !gradeCodes.contains(mappedStudentRecord.getGradeCode().trim().toUpperCase())) {
          log.debug("updated grade code to null from :: {} at index {}, for studNoLike {}", mappedStudentRecord.getGradeCode(), index, studNoLike);
          mappedStudentRecord.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
        }
        if (mappedStudentRecord.getLegalLastName() == null || mappedStudentRecord.getLegalLastName().trim().equals("")) {
          mappedStudentRecord.setLegalLastName("NULL");
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
    log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
    return true;
  }

  @Retryable(value = {Exception.class}, backoff = @Backoff(multiplier = 2, delay = 2000))
  public boolean processDemographicsAuditEntities(List<PenAuditEntity> penAuditEntities, String penLike) {
    var currentLotSize = penAuditEntities.size();
    List<StudentHistoryEntity> studentHistoryEntities = new ArrayList<>();
    var index = 1;
    for (var penAuditEntity : penAuditEntities) {
      if (penAuditEntity.getPen() != null && penAuditEntity.getDob() != null) {
        log.debug("Total Records :: {} , processing pen :: {} at index {}, for penLike {}", currentLotSize, penAuditEntity.getPen(), index, penLike);
        StudentEntity studentEntity = studentPersistenceService.getStudentByPen(penAuditEntity.getPen().trim());
        if (studentEntity != null && studentEntity.getStudentID() != null) {
          var studentHistory = PEN_AUDIT_STUDENT_HISTORY_MAPPER.toStudentHistory(penAuditEntity);
          studentHistory.setStudentID(studentEntity.getStudentID());
          if (studentHistory.getGradeCode() != null && !gradeCodes.contains(studentHistory.getGradeCode().trim().toUpperCase())) {
            log.debug("updated grade code to null from :: {} at index {}, for penLike {}", studentHistory.getGradeCode(), index, penLike);
            studentHistory.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
          }
          if (StringUtils.isBlank(studentHistory.getLegalLastName())) {
            studentHistory.setLegalLastName("NULL");
          }
          studentHistoryEntities.add(studentHistory);
        } else {
          log.error("No Student record found for pen :: {}", penAuditEntity.getPen());
        }
        index++;
      } else {
        log.error("NO PEN AND STUD BIRTH skipping this record at index {}, for penLike {}", index, penLike);
      }
    }

    if (!studentHistoryEntities.isEmpty()) {
      try {
        studentPersistenceService.saveStudentHistory(studentHistoryEntities);
        log.debug("processing complete for penLike :: {}, persisted {} records into DB", penLike, studentHistoryEntities.size());
      } catch (final Exception ex) {
        log.error("Exception while persisting records for penLike :: {}, records into DB , exception is :: {}", penLike, ex);
        throw ex;
      }
    }
    log.info("total number of history records processed :: {}", CounterUtil.historyProcessCounter.incrementAndGet());
    return true;
  }


  private String getFormattedDOB(String dob) {
    var year = dob.substring(0, 4);
    var month = dob.substring(4, 6);
    var day = dob.substring(6, 8);

    return year.concat("-").concat(month).concat("-").concat(day);
  }
}
