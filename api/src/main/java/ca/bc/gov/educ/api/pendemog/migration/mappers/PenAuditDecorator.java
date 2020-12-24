package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.constants.HistoryActivityCode;
import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
    entity.setDob(getLocalDateFromString(penAuditEntity.getDob()));
    return entity;
  }

  private LocalDateTime getLocalDateTimeFromString(String dateTime) {
    var pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(dateTime, pattern);
  }

  private LocalDate getLocalDateFromString(String date) {
    var pattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    return LocalDate.parse(date, pattern);
  }

  private String getHistoryActivityCode(String auditCode) {
    if (auditCode != null) {
      return auditCode.trim().equalsIgnoreCase("A") ? HistoryActivityCode.USER_NEW.getCode() : HistoryActivityCode.USER_EDIT.getCode();
    }
    return HistoryActivityCode.USER_NEW.getCode();
  }
}
