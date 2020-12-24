package ca.bc.gov.educ.api.pendemog.migration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "PEN_AUDIT@PENLINK.WORLD")
public class PenAuditEntity {

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

}
