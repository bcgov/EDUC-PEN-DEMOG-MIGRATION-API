package ca.bc.gov.educ.api.pendemog.migration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "PEN_AUDIT@PENLINK.WORLD")
@IdClass(PenAuditEntity.PenAuditID.class)
public class PenAuditEntity implements Serializable {

  @Id
  @Column(name = "ACTIVITY_DATE", insertable = false, updatable = false)
  LocalDateTime activityDate;

  @Id
  @Column(name = "AUDIT_CODE", insertable = false, updatable = false)
  String auditCode;

  @Id
  @Column(name = "PEN_LOCAL_ID", insertable = false, updatable = false)
  String localID;

  @Id
  @Column(name = "PEN_MINCODE", insertable = false, updatable = false)
  String mincode;

  @Id
  @Column(name = "POSTAL", insertable = false, updatable = false)
  String postalCode;

  @Id
  @Column(name = "STUD_BIRTH", insertable = false, updatable = false)
  LocalDate dob;

  @Id
  @Column(name = "STUD_DEMOG_CODE", insertable = false, updatable = false)
  String demogCode;

  @Id
  @Column(name = "STUD_STATUS", insertable = false, updatable = false)
  String statusCode;

  @Id
  @Column(name = "STUD_NO", insertable = false, updatable = false)
  String pen;

  @Id
  @Column(name = "STUD_GIVEN", insertable = false, updatable = false)
  String legalFirstName;

  @Id
  @Column(name = "STUD_MIDDLE", insertable = false, updatable = false)
  String legalMiddleNames;

  @Id
  @Column(name = "STUD_SURNAME", insertable = false, updatable = false)
  String legalLastName;

  @Id
  @Column(name = "STUD_SEX", insertable = false, updatable = false)
  String sexCode;

  @Id
  @Column(name = "STUD_SEX", insertable = false, updatable = false)
  String genderCode;

  @Id
  @Column(name = "USUAL_GIVEN", insertable = false, updatable = false)
  String usualFirstName;

  @Id
  @Column(name = "USUAL_MIDDLE", insertable = false, updatable = false)
  String usualMiddleNames;

  @Id
  @Column(name = "USUAL_SURNAME", insertable = false, updatable = false)
  String usualLastName;

  @Id
  @Column(name = "STUD_GRADE", insertable = false, updatable = false)
  String gradeCode;

  @Id
  @Column(name = "STUD_GRADE_YEAR", insertable = false, updatable = false)
  String gradeYear;

  @Id
  @Column(name = "USER_NAME", insertable = false, updatable = false)
  String createUser;

  @Id
  @Column(name = "ACTIVITY_DATE", insertable = false, updatable = false)
  LocalDateTime createDate;

  @Id
  @Column(name = "USER_NAME", insertable = false, updatable = false)
  String updateUser;

  @Id
  @Column(name = "ACTIVITY_DATE", insertable = false, updatable = false)
  LocalDateTime updateDate;

  public PenAuditID getId() {
    return new PenAuditID(
        this.activityDate, this.auditCode, this.localID, this.mincode, this.postalCode, this.dob, this.demogCode, this.statusCode, this.pen, this.legalFirstName, this.legalMiddleNames,
        this.legalLastName, this.sexCode, this.genderCode, this.usualFirstName, this.usualMiddleNames, this.usualLastName, this.gradeCode, this.gradeYear, this.createUser, this.createDate,
        this.updateUser, this.updateDate);
  }

  public void setId(PenAuditID id) {
    this.activityDate = id.getActivityDate();
    this.auditCode = id.getAuditCode();
    this.localID = id.getLocalID();
    this.mincode = id.getMincode();
    this.postalCode = id.getPostalCode();
    this.dob = id.getDob();
    this.demogCode = id.getDemogCode();
    this.statusCode = id.getStatusCode();
    this.pen = id.getPen();
    this.legalFirstName = id.getLegalFirstName();
    this.legalMiddleNames = id.getLegalMiddleNames();
    this.legalLastName = id.getLegalLastName();
    this.sexCode = id.getSexCode();
    this.genderCode = id.getGenderCode();
    this.usualFirstName = id.getUsualFirstName();
    this.usualMiddleNames = id.getUsualMiddleNames();
    this.usualLastName = id.getUsualLastName();
    this.gradeCode = id.getGradeCode();
    this.gradeYear = id.getGradeYear();
    this.createUser = id.getCreateUser();
    this.createDate = id.getCreateDate();
    this.updateUser = id.getUpdateUser();
    this.updateDate = id.getUpdateDate();
  }

  @AllArgsConstructor
  @Data
  @Builder
  public static class PenAuditID implements Serializable {

    LocalDateTime activityDate;
    String auditCode;
    String localID;
    String mincode;
    String postalCode;
    LocalDate dob;
    String demogCode;
    String statusCode;
    String pen;
    String legalFirstName;
    String legalMiddleNames;
    String legalLastName;
    String sexCode;
    String genderCode;
    String usualFirstName;
    String usualMiddleNames;
    String usualLastName;
    String gradeCode;
    String gradeYear;
    String createUser;
    LocalDateTime createDate;
    String updateUser;
    LocalDateTime updateDate;
  }
}
