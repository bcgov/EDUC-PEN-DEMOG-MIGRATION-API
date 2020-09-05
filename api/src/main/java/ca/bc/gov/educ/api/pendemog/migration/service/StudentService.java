package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.constants.GradeCodes;
import ca.bc.gov.educ.api.pendemog.migration.mappers.PenDemogStudentMapper;
import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.StudentRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StudentService {
  private final Set<String> gradeCodes = new HashSet<>();
  private static final PenDemogStudentMapper studentMapper = PenDemogStudentMapper.mapper;
  private final StudentRepository studentRepository;

  @Autowired
  public StudentService(StudentRepository studentRepository) {
    this.studentRepository = studentRepository;
  }

  @PostConstruct
  public void init() {
    gradeCodes.addAll(Arrays.stream(GradeCodes.values()).map(GradeCodes::getCode).collect(Collectors.toSet()));
    log.info("Added all the grade codes {}", gradeCodes.size());
  }

  @Retryable(value = {Exception.class}, backoff = @Backoff(multiplier = 2, delay = 2000))
  public boolean processDemographicsEntities(List<PenDemographicsEntity> penDemographicsEntities, String surNameLike) {
    var currentLotSize = penDemographicsEntities.size();
    List<StudentEntity> studentEntities = new ArrayList<>();
    var index = 1;
    for (var penDemog : penDemographicsEntities) {
      if (penDemog.getStudNo() != null && penDemog.getStudBirth() != null) {
        log.info("Total Records :: {} , processing pen :: {} at index {}, for surname {}", currentLotSize, penDemog.getStudNo(), index, surNameLike);
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
          log.info("updated grade code to null from :: {} at index {}, for surname {}", mappedStudentRecord.getGradeCode(), index, surNameLike);
          mappedStudentRecord.setGradeCode(null);// to maintain FK, it is ok to put null but not OK to put blank string or anything which is not present in DB.
        }
        mappedStudentRecord.setCreateDate(LocalDateTime.now());
        mappedStudentRecord.setUpdateDate(LocalDateTime.now());
        studentEntities.add(mappedStudentRecord);
      } else {
        log.error("NO PEN AND STUD BIRTH skipping this record at index {}, for surname {}", index, surNameLike);
      }
      index++;
    }
    if (!studentEntities.isEmpty()) {
      studentRepository.saveAll(studentEntities);
      log.info("processing complete for surNameLike :: {}, persisted {} records into DB", surNameLike, studentEntities.size());
    }
    return true;
  }

  private String getFormattedDOB(String dob) {
    var year = dob.substring(0, 4);
    var month = dob.substring(4, 6);
    var day = dob.substring(6, 8);

    return year.concat("-").concat(month).concat("-").concat(day);
  }

}
