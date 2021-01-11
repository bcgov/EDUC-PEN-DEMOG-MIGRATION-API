package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public  abstract class PenDemogDecorator implements PenDemogStudentMapper {
  private final PenDemogStudentMapper delegate;

  protected PenDemogDecorator(PenDemogStudentMapper mapper) {
    this.delegate = mapper;
  }

  @Override
  public StudentEntity toStudent(PenDemographicsEntity penDemographicsEntity) {
    var entity = delegate.toStudent(penDemographicsEntity);
    if (entity == null) {
      return null;
    }
    entity.setCreateDate(getLocalDateTimeFromString(penDemographicsEntity.getCreateDate()));
    entity.setUpdateDate(getLocalDateTimeFromString(penDemographicsEntity.getUpdateDate()));
    entity.setPostalCode(formatPostalCode(penDemographicsEntity.getPostalCode()));
    return entity;
  }


  private String formatPostalCode(String postalCode) {
    if (postalCode == null) {
      return null;
    } else {
      postalCode = postalCode.replaceAll("\\s", "");
    }
    if (postalCode.length() > 6) {
      postalCode = postalCode.substring(0, 6);
    }
    return postalCode;
  }

  private LocalDateTime getLocalDateTimeFromString(String dateTime) {
    if (dateTime == null) {
      return null;
    } else {
      dateTime = dateTime.trim();
      if (StringUtils.length(dateTime) > 19) {
        dateTime = dateTime.substring(0, 19);
      }
    }
    var pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    try {
      return LocalDateTime.parse(dateTime, pattern);
    } catch (final DateTimeParseException exception) {
      log.error("system will use current date time as parsing error of date :: {}, error :: {}", dateTime, exception);
    }
    return LocalDateTime.now();
  }

}
