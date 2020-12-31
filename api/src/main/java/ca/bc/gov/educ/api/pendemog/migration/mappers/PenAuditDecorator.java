package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.constants.HistoryActivityCode;
import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public abstract class PenAuditDecorator implements PenAuditStudentHistoryMapper {
  private final PenAuditStudentHistoryMapper delegate;

  protected PenAuditDecorator(PenAuditStudentHistoryMapper mapper) {
    this.delegate = mapper;
  }

  @Override
  public StudentHistoryEntity toStudentHistory(PenAuditEntity penAuditEntity) {
    var entity = delegate.toStudentHistory(penAuditEntity);
    if (entity == null) {
      return null;
    }
    entity.setCreateDate(getLocalDateTimeFromString(penAuditEntity.getActivityDate()));
    entity.setUpdateDate(getLocalDateTimeFromString(penAuditEntity.getActivityDate()));
    entity.setHistoryActivityCode(getHistoryActivityCode(penAuditEntity.getAuditCode()));
    entity.setDob(getDobFromString(penAuditEntity.getDob()));
    entity.setPostalCode(formatPostalCode(entity.getPostalCode()));
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

  private LocalDate getDobFromString(String dob) {
    if (dob == null) {
      log.error("system will use current date as dob was null");
      return LocalDate.now();
    } else {
      dob = dob.trim();
      if (StringUtils.length(dob) > 8) {
        dob = dob.substring(0, 8);
      }
    }
    var pattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    try {
      return LocalDate.parse(dob, pattern);
    } catch (final DateTimeParseException exception) {
      log.error("system will use current date as parsing error of dob :: {}, error :: {}", dob, exception);
    }
    return LocalDate.now();
  }

  private String getHistoryActivityCode(String auditCode) {
    if (auditCode != null) {
      return auditCode.trim().equalsIgnoreCase("A") ? HistoryActivityCode.USER_NEW.getCode() : HistoryActivityCode.USER_EDIT.getCode();
    }
    return HistoryActivityCode.USER_NEW.getCode();
  }
}
