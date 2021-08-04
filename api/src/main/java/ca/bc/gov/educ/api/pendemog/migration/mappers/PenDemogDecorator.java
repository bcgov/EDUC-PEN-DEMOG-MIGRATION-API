package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public  abstract class PenDemogDecorator implements PenDemogStudentMapper {
  private final PenDemogStudentMapper delegate;

  protected PenDemogDecorator(final PenDemogStudentMapper mapper) {
    this.delegate = mapper;
  }

  @Override
  public StudentEntity toStudent(final PenDemographicsEntity penDemographicsEntity) {
    final var entity = this.delegate.toStudent(penDemographicsEntity);
    if (entity == null) {
      return null;
    }
    entity.setCreateDate(this.getLocalDateTimeFromString(penDemographicsEntity.getCreateDate(), entity.getPen()));
    entity.setUpdateDate(this.getLocalDateTimeFromString(penDemographicsEntity.getUpdateDate(), entity.getPen()));
    entity.setPostalCode(this.formatPostalCode(penDemographicsEntity.getPostalCode()));
    return entity;
  }

@Override
public void updateStudent(final PenDemographicsEntity penDemographicsEntity, final StudentEntity entity) {
  this.delegate.updateStudent(penDemographicsEntity, entity);
  entity.setCreateDate(this.getLocalDateTimeFromString(penDemographicsEntity.getCreateDate(), entity.getPen()));
  entity.setUpdateDate(this.getLocalDateTimeFromString(penDemographicsEntity.getUpdateDate(), entity.getPen()));
  entity.setPostalCode(this.formatPostalCode(penDemographicsEntity.getPostalCode()));
}
  private String formatPostalCode(String postalCode) {
    if (postalCode == null) {
      return null;
    } else {
      postalCode = postalCode.replaceAll("\\s", "");
    }
    if (postalCode.length() > 7) {
      postalCode = postalCode.substring(0, 7);
    }
    return postalCode;
  }

  private LocalDateTime getLocalDateTimeFromString(String dateTime, final String pen) {
    if (StringUtils.isBlank(dateTime)) {
      log.error("system will use current date time for create/update date as it is null for pen :: {}", pen);
      return LocalDateTime.now();
    } else {
      dateTime = dateTime.trim();
      if (StringUtils.length(dateTime) > 19) {
        dateTime = dateTime.substring(0, 19);
      }
    }
    final var pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    try {
      return LocalDateTime.parse(dateTime, pattern);
    } catch (final DateTimeParseException exception) {
      log.error("system will use current date time as parsing error of date :: {}, for pen :: {}", dateTime, pen);
    }
    return LocalDateTime.now();
  }

}
