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

  protected PenAuditDecorator(final PenAuditStudentHistoryMapper mapper) {
    this.delegate = mapper;
  }

  @Override
  public StudentHistoryEntity toStudentHistory(final PenAuditEntity penAuditEntity) {
    final var entity = this.delegate.toStudentHistory(penAuditEntity);
    if (entity == null) {
      return null;
    }
    entity.setCreateDate(this.getLocalDateTimeFromString(penAuditEntity.getActivityDate(), entity.getPen()));
    entity.setUpdateDate(this.getLocalDateTimeFromString(penAuditEntity.getActivityDate(), entity.getPen()));
    entity.setHistoryActivityCode(this.getHistoryActivityCode(penAuditEntity.getAuditCode()));
    entity.setDob(this.getDobFromString(penAuditEntity.getDob(), entity.getPen()));
    entity.setPostalCode(this.formatPostalCode(entity.getPostalCode()));
    if (StringUtils.isBlank(entity.getCreateUser())) {
      entity.setCreateUser("PEN_MIGRATION_API");
    }
    if (StringUtils.isBlank(entity.getUpdateUser())) {
      entity.setUpdateUser("PEN_MIGRATION_API");
    }
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
      log.info("system will use current date time as parsing error of date :: {}, pen :: {}", dateTime, pen);
    }
    return LocalDateTime.now();
  }

  private LocalDate getDobFromString(String dob, final String pen) {
    if (StringUtils.isBlank(dob)) {
      log.error("Data Quality Issue, DOB was blank, Setting DOB to current date for pen :: {}", pen);
      return LocalDate.now();
    } else {
      dob = dob.trim();
      if (StringUtils.length(dob) > 8) {
        dob = dob.substring(0, 8);
      }
    }
    final var pattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    try {
      return LocalDate.parse(dob, pattern);
    } catch (final DateTimeParseException exception) {
      log.error("Data Quality Issue, BirthDate could not be parsed dob :: {}, pen :: {}", dob, pen); // pen is used for
      // logging purpose.
    }
    return LocalDate.now();
  }

  private String getHistoryActivityCode(final String auditCode) {
    if (auditCode != null) {
      return auditCode.trim().equalsIgnoreCase("A") ? HistoryActivityCode.REQ_NEW.getCode() : HistoryActivityCode.USER_EDIT.getCode();
    }
    return HistoryActivityCode.REQ_NEW.getCode();
  }
}
