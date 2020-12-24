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
  @Column(name = "ACTIVITY_DATE")
  LocalDateTime activityDate;

  @Id
  @Column(name = "AUDIT_CODE")
  String auditCode;

  @Id
  @Column(name = "PEN_LOCAL_ID")
  String localID;

  @Id
  @Column(name = "PEN_MINCODE")
  String mincode;

  @Id
  @Column(name = "POSTAL")
  String postalCode;

  @Id
  @Column(name = "STUD_BIRTH")
  LocalDate dob;

  @Id
  @Column(name = "STUD_DEMOG_CODE")
  String demogCode;

  @Id
  @Column(name = "STUD_STATUS")
  String statusCode;

  @Id
  @Column(name = "STUD_NO")
  String pen;

  @Id
  @Column(name = "STUD_GIVEN")
  String legalFirstName;

  @Id
  @Column(name = "STUD_MIDDLE")
  String legalMiddleNames;

  @Id
  @Column(name = "STUD_SURNAME")
  String legalLastName;

  @Id
  @Column(name = "STUD_SEX")
  String sexCode;

  @Id
  @Column(name = "STUD_SEX")
  String genderCode;

  @Id
  @Column(name = "USUAL_GIVEN")
  String usualFirstName;

  @Id
  @Column(name = "USUAL_MIDDLE")
  String usualMiddleNames;

  @Id
  @Column(name = "USUAL_SURNAME")
  String usualLastName;

  @Id
  @Column(name = "STUD_GRADE")
  String gradeCode;

  @Id
  @Column(name = "STUD_GRADE_YEAR")
  String gradeYear;

  @Id
  @Column(name = "USER_NAME", updatable = false)
  String createUser;

  @Id
  @Column(name = "ACTIVITY_DATE")
  LocalDateTime createDate;

  @Id
  @Column(name = "USER_NAME")
  String updateUser;

  @Id
  @Column(name = "ACTIVITY_DATE")
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
