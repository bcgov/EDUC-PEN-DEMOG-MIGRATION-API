package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.DemogCodes;
import ca.bc.gov.educ.api.pendemog.migration.constants.GradeCodes;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenAuditStudentHistoryMapper;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenDemogStudentMapper;
import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@Slf4j
public class StudentService {
  private static final PenDemogStudentMapper studentMapper = PenDemogStudentMapper.mapper;
  private static final PenAuditStudentHistoryMapper PEN_AUDIT_STUDENT_HISTORY_MAPPER = PenAuditStudentHistoryMapper.mapper;
  private final Set<String> gradeCodes = new HashSet<>();
  private final Set<String> demogCodes = new HashSet<>();
  private final StudentPersistenceService studentPersistenceService;

  @Autowired
  public StudentService(final StudentPersistenceService studentPersistenceService) {
    this.studentPersistenceService = studentPersistenceService;
  }

  @PostConstruct
  public void init() {
    this.gradeCodes.addAll(Arrays.stream(GradeCodes.values()).map(GradeCodes::getCode).collect(Collectors.toSet()));
    this.demogCodes.addAll(Arrays.stream(DemogCodes.values()).map(DemogCodes::getCode).collect(Collectors.toSet()));
    log.info("Added all the grade codes {}", this.gradeCodes.size());
    log.info("Added all the demog codes {}", this.demogCodes.size());
  }

  public boolean processDemographicsEntities(final List<PenDemographicsEntity> penDemographicsEntities, final String studNoLike, final Map<String, StudentEntity> studentEntityMap) {
    final var currentLotSize = penDemographicsEntities.size();
    if (currentLotSize > 15000) {
      final var chunks = Lists.partition(penDemographicsEntities, 10000);
      for (final var chunk : chunks) {
        this.processChunk(chunk, studNoLike, currentLotSize, studentEntityMap);
      }
    } else {
      this.processChunk(penDemographicsEntities, studNoLike, currentLotSize, studentEntityMap);
    }


    log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
    return true;
  }

  private void processChunk(final List<PenDemographicsEntity> penDemographicsEntities, final String studNoLike, final int currentLotSize, final Map<String, StudentEntity> studentEntityMap) {
    final List<StudentEntity> studentEntities = new ArrayList<>();
    var index = 1;
    for (final var penDemographics : penDemographicsEntities) {
      if (penDemographics.getStudNo() != null && penDemographics.getStudBirth() != null) {
        val penDemog = PenDemogStudentMapper.mapper.toTrimmedEntity(penDemographics);
        penDemog.setStudBirth(this.getFormattedDOB(penDemog.getStudBirth())); //update the format
        log.debug("Total Records :: {} , processing pen :: {} at index {}, for studNoLike {}", currentLotSize, penDemog.getStudNo(), index, studNoLike);

        final StudentEntity mappedStudentRecord;
        if (studentEntityMap.containsKey(penDemog.getStudNo())) {
          val currentStudent = studentEntityMap.get(penDemog.getStudNo());
          PenDemogStudentMapper.mapper.updateStudent(penDemog, currentStudent);
          if (!(StringUtils.equalsIgnoreCase(currentStudent.getDemogCode(), DemogCodes.CONFIRMED.getCode()))) {
            currentStudent.setDemogCode(StringUtils.isNotBlank(penDemog.getDemogCode()) ? penDemog.getDemogCode().trim() : DemogCodes.ACCEPTED.getCode());
          }
          mappedStudentRecord = currentStudent;
        } else {
          mappedStudentRecord = studentMapper.toStudent(penDemog);
        }

        try {
          final LocalDate dob = LocalDate.parse(penDemog.getStudBirth());
          mappedStudentRecord.setDob(dob);
        } catch (final Exception e) {
          mappedStudentRecord.setDob(LocalDate.parse("2000-01-01"));
        }
        if (mappedStudentRecord.getGradeCode() != null && !this.gradeCodes.contains(mappedStudentRecord.getGradeCode().trim().toUpperCase())) {
          log.debug("updated grade code to null from :: {} at index {}, for studNoLike {}", mappedStudentRecord.getGradeCode(), index, studNoLike);
          mappedStudentRecord.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
        }
        if (mappedStudentRecord.getLegalLastName() == null || mappedStudentRecord.getLegalLastName().trim().equals("")) {
          mappedStudentRecord.setLegalLastName("NULL");
        }
        if (StringUtils.isBlank(mappedStudentRecord.getCreateUser())) {
          log.debug("updating create user from null or blank");
          mappedStudentRecord.setCreateUser("PEN_MIGRATION_API");
        }
        if (StringUtils.isBlank(mappedStudentRecord.getUpdateUser())) {
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
        this.studentPersistenceService.saveStudents(studentEntities);
        log.debug("processing complete for studNoLike :: {}, persisted {} records into DB", studNoLike, studentEntities.size());
      } catch (final Exception ex) {
        log.error("Exception while persisting records for studNoLike :: {}, records into DB , exception is :: {}", studNoLike, ex);
        throw ex;
      }
    }
  }


  public List<StudentHistoryEntity> processDemographicsAuditEntities(final List<PenAuditEntity> penAuditEntities, final Map<String, UUID> penStudIDMap) {
    final var currentLotSize = penAuditEntities.size();
    final List<StudentHistoryEntity> studentHistoryEntities = new ArrayList<>();
    final AtomicInteger recordCount = new AtomicInteger(0);
    for (final var penAuditEntity : penAuditEntities) {
      recordCount.incrementAndGet();

      if (penAuditEntity != null && penAuditEntity.getPen() != null) {
        log.debug("Total Records :: {} , processing audit entity :: {} at index {}, for pen {}", currentLotSize, penAuditEntity, recordCount.get(), penAuditEntity.getPen());
        try {
          final var studentHistory = PEN_AUDIT_STUDENT_HISTORY_MAPPER.toStudentHistory(penAuditEntity);
          studentHistory.setStudentID(penStudIDMap.get(studentHistory.getPen()));
          if (studentHistory.getGradeCode() != null && !this.gradeCodes.contains(studentHistory.getGradeCode().trim().toUpperCase())) {
            log.trace("updated grade code to null from :: {} at index {}, for pen {}", studentHistory.getGradeCode(),
              recordCount.get(), penAuditEntity.getPen());
            studentHistory.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
          }
          if (studentHistory.getDemogCode() != null && !this.demogCodes.contains(studentHistory.getDemogCode().trim().toUpperCase())) {
            log.trace("updated demog code to null from :: {} at index {}, for pen {}", studentHistory.getDemogCode(),
              recordCount.get(), penAuditEntity.getPen());
            studentHistory.setDemogCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or
            // anything which is not present in DB.
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


  private String getFormattedDOB(final String dob) {
    final var year = dob.substring(0, 4);
    final var month = dob.substring(4, 6);
    final var day = dob.substring(6, 8);

    return year.concat("-").concat(month).concat("-").concat(day);
  }

  public void saveHistoryEntities(final List<StudentHistoryEntity> historyEntitiesToPersist) {
    this.studentPersistenceService.saveStudentHistory(historyEntitiesToPersist);
  }
}
