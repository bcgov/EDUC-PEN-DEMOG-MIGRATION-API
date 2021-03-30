package ca.bc.gov.educ.api.pendemog.migration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "API_STUDENT.RDB_PEN_AUDIT")
@IdClass(PenAuditPK.class)
public class PenAuditEntity implements Serializable {

  @Id
  @Column(name = "ACTIVITY_DATE", insertable = false, updatable = false)
  String activityDate;

  @Id
  @Column(name = "AUDIT_CODE", insertable = false, updatable = false)
  String auditCode;

  @Column(name = "PEN_LOCAL_ID", insertable = false, updatable = false)
  String localID;

  @Column(name = "PEN_MINCODE", insertable = false, updatable = false)
  String mincode;

  @Column(name = "POSTAL", insertable = false, updatable = false)
  String postalCode;

  @Column(name = "STUD_BIRTH", insertable = false, updatable = false)
  String dob;

  @Column(name = "STUD_DEMOG_CODE", insertable = false, updatable = false)
  String demogCode;

  @Column(name = "STUD_STATUS", insertable = false, updatable = false)
  String statusCode;

  @Id
  @Column(name = "STUD_NO", insertable = false, updatable = false)
  String pen;

  @Column(name = "STUD_GIVEN", insertable = false, updatable = false)
  String legalFirstName;

  @Column(name = "STUD_MIDDLE", insertable = false, updatable = false)
  String legalMiddleNames;

  @Column(name = "STUD_SURNAME", insertable = false, updatable = false)
  String legalLastName;

  @Column(name = "STUD_SEX", insertable = false, updatable = false)
  String sexCode;

  @Column(name = "STUD_SEX", insertable = false, updatable = false)
  String genderCode;

  @Column(name = "USUAL_GIVEN", insertable = false, updatable = false)
  String usualFirstName;

  @Column(name = "USUAL_MIDDLE", insertable = false, updatable = false)
  String usualMiddleNames;

  @Column(name = "USUAL_SURNAME", insertable = false, updatable = false)
  String usualLastName;

  @Column(name = "STUD_GRADE", insertable = false, updatable = false)
  String gradeCode;

  @Column(name = "STUD_GRADE_YEAR", insertable = false, updatable = false)
  String gradeYear;

  @Column(name = "USER_NAME", insertable = false, updatable = false)
  String createUser;


  @Column(name = "USER_NAME", insertable = false, updatable = false)
  String updateUser;


  public PenAuditPK getId() {
    return new PenAuditPK(
        this.activityDate, this.auditCode, this.pen);
  }

  public void setId(PenAuditPK id) {
    this.activityDate = id.getActivityDate();
    this.auditCode = id.getAuditCode();
    this.pen = id.getPen();
  }


}
