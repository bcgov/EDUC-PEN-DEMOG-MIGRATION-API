package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.DemogCodes;
import ca.bc.gov.educ.api.pendemog.migration.constants.GradeCodes;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenAuditStudentHistoryMapper;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenDemogStudentMapper;
import ca.bc.gov.educ.api.pendemog.migration.model.*;
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
  private final Set<String> statusCodes = new HashSet<>();
  private final StudentPersistenceService studentPersistenceService;

  @Autowired
  public StudentService(final StudentPersistenceService studentPersistenceService) {
    this.studentPersistenceService = studentPersistenceService;
  }

  @PostConstruct
  public void init() {
    this.gradeCodes.addAll(Arrays.stream(GradeCodes.values()).map(GradeCodes::getCode).collect(Collectors.toSet()));
    this.demogCodes.addAll(Arrays.stream(DemogCodes.values()).map(DemogCodes::getCode).collect(Collectors.toSet()));
    statusCodes.add("A");
    statusCodes.add("D");
    statusCodes.add("M");
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
          log.error("Data Quality Issue, Setting Birthdate of Student to 2000-01-01  as dob :: {} could not be parsed for pen :: {}", penDemog.getStudBirth(), mappedStudentRecord.getPen());
          mappedStudentRecord.setDob(LocalDate.parse("2000-01-01"));
        }
        if (mappedStudentRecord.getGradeCode() != null && !this.gradeCodes.contains(mappedStudentRecord.getGradeCode().trim().toUpperCase())) {
          log.debug("updated grade code to null from :: {} at index {}, for studNoLike {}", mappedStudentRecord.getGradeCode(), index, studNoLike);
          mappedStudentRecord.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
        }
        if (StringUtils.isBlank(mappedStudentRecord.getLegalLastName())) {
          log.error("Data Quality Issue, Setting Legal Last Name of Student to NULL for pen :: {}", mappedStudentRecord.getPen());
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
        if (StringUtils.isBlank(mappedStudentRecord.getDemogCode())) {
          log.error("Data Quality Issue, Setting Demog code of Student to A for pen :: {}", mappedStudentRecord.getPen());
          mappedStudentRecord.setDemogCode("A");
        }
        if (StringUtils.isBlank(mappedStudentRecord.getStatusCode())) {
          log.error("Data Quality Issue, Setting Status code of Student to A for pen :: {}", mappedStudentRecord.getPen());
          mappedStudentRecord.setStatusCode("A");
        }
        studentEntities.add(mappedStudentRecord);
      } else {
        log.error("NO PEN OR STUD BIRTH skipping this record at index {}, for studNoLike {}", index, studNoLike);
      }
      index++;
    }
    if (!studentEntities.isEmpty()) {
      try {
        this.studentPersistenceService.saveStudents(studentEntities);
        log.debug("processing complete for studNoLike :: {}, persisted {} records into DB", studNoLike, studentEntities.size());
      } catch (final Exception ex) {
        log.error("Exception while persisting records for studNoLike :: {}, exception is :: {}", studNoLike, ex);
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
            log.trace("updated grade code to null in Audit from :: {} at index {}, for pen {}", studentHistory.getGradeCode(),
              recordCount.get(), penAuditEntity.getPen());
            studentHistory.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
          }
          if (studentHistory.getDemogCode() != null && !this.demogCodes.contains(studentHistory.getDemogCode().trim().toUpperCase())) {
            if (StringUtils.isNotBlank(StringUtils.trim(studentHistory.getDemogCode()))) {
              log.error("Data Quality Issue in Audit, Setting Demog code to to null as demog code :: {} is not recognized for pen :: {}",studentHistory.getDemogCode(), studentHistory.getPen());
            }
            studentHistory.setDemogCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or
            // anything which is not present in DB.
          }
          if (StringUtils.isBlank(studentHistory.getLegalLastName())) {
            log.error("Data Quality Issue in Audit, Setting Legal Last Name of Student to NULL for pen :: {}", penAuditEntity.getPen());
            studentHistory.setLegalLastName("NULL");
          }
          if(studentHistory.getDob().isAfter(LocalDate.now())){
            log.error("Data Quality Issue in Audit, Setting DOB to current date as DOB is future date for pen :: {}", penAuditEntity.getPen());
            studentHistory.setDob(LocalDate.now());
          }
          if(StringUtils.isBlank(studentHistory.getStatusCode())){
            log.error("Data Quality Issue in Audit, Setting status to 'A' as it was blank or null for pen  :: {}", penAuditEntity.getPen());
            studentHistory.setStatusCode("A");
          }else if(!this.statusCodes.contains(studentHistory.getStatusCode().trim().toUpperCase())){
            log.error("Data Quality Issue in Audit, Setting status to 'A' as status :: {} is not recognized for pen :: {}",studentHistory.getStatusCode().trim(), penAuditEntity.getPen());
            studentHistory.setStatusCode("A");
          }
          studentHistoryEntities.add(studentHistory);
        } catch (final Exception ex) {
          log.error("exception while processing pen audit entity :: {}, {}", penAuditEntity, ex);
        }
      } else {
        log.error("Could not process the audit entity at {} as it is null or the pen is null :: {}", recordCount.get(), penAuditEntity != null ? penAuditEntity.toString() : "");
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

    try {
      this.studentPersistenceService.saveStudentHistory(historyEntitiesToPersist);
    } catch (Exception e) {
      // EMPTY, it is already logged.
    }
  }

  public void saveMergesAndStudentUpdates(final List<StudentMergeEntity> mergeFromEntities, final List<StudentMergeEntity> mergeTOEntities, final List<StudentEntity> mergedStudents) {
    try {
      this.studentPersistenceService.saveMergesAndStudentUpdates(mergeFromEntities, mergeTOEntities, mergedStudents);
    } catch (Exception e) {
      //Empty
    }
  }

  public void updateStudentWithMemos(List<StudentEntity> memoStudents) {
    this.studentPersistenceService.updateStudentWithMemos(memoStudents);
  }
}
