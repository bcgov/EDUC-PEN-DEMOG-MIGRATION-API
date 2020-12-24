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
import java.time.LocalDateTime;

/**
 * The type Pen demographics entity.
 */
@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "PEN_DEMOG@PENLINK.WORLD")
public class PenDemographicsEntity {

  @Id
  @Column(name = "STUD_NO")
  private String studNo;

  @Column(name = "STUD_SURNAME")
  private String studSurname;

  @Column(name = "STUD_GIVEN")
  private String studGiven;

  @Column(name = "STUD_MIDDLE")
  private String studMiddle;

  @Column(name = "USUAL_SURNAME")
  private String usualSurname;

  @Column(name = "USUAL_GIVEN")
  private String usualGiven;

  @Column(name = "USUAL_MIDDLE")
  private String usualMiddle;

  @Column(name = "STUD_BIRTH")
  private String studBirth;

  @Column(name = "STUD_SEX")
  private String studSex;

  @Column(name = "STUD_STATUS")
  private String studStatus;

  @Column(name = "PEN_LOCAL_ID")
  private String localID;

  @Column(name = "POSTAL")
  private String postalCode;

  @Column(name = "STUD_GRADE")
  private String grade;

  @Column(name = "STUD_GRADE_YEAR")
  private String gradeYear;

  @Column(name = "STUD_DEMOG_CODE")
  private String demogCode;

  @Column(name = "PEN_MINCODE")
  private String mincode;

  @Column(name = "CREATE_DATE")
  private String createDate;

  @Column(name = "CREATE_USER_NAME")
  private String createUser;

  @Column(name = "UPDATE_DATE")
  private String updateDate;

  @Column(name = "UPDATE_USER_NAME")
  private String updateUser;
  //below are added for further data pull

  @Column(name = "STUD_TRUE_NO")
  private String studentTrueNo;

  @Column(name = "MERGE_TO_USER_NAME")
  private String mergeToUserName;

  @Column(name = "MERGE_TO_CODE")
  private String mergeToCode;

  @Column(name = "MERGE_TO_DATE")
  private String mergeToDate;
}
