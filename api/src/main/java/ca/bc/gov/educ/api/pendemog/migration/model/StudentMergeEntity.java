package ca.bc.gov.educ.api.pendemog.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PastOrPresent;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * The type Student merge entity.
 */
@Entity
@Table(name = "API_PEN_VALIDATION.STUDENT_MERGE")
@Data
@DynamicUpdate
@JsonIgnoreProperties(ignoreUnknown = true)
public class StudentMergeEntity {
  /**
   * The Student merge id.
   */
  @Id
  @GeneratedValue(generator = "UUID")
  @GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator", parameters = {
      @Parameter(name = "uuid_gen_strategy_class", value = "org.hibernate.id.uuid.CustomVersionOneStrategy")})
  @Column(name = "STUDENT_MERGE_ID", unique = true, updatable = false, columnDefinition = "BINARY(16)")
  UUID studentMergeID;

  /**
   * The Student id.
   */
//To keep the code simple, we didn't use the @ManyToOne association here
  @NotNull(message = "studentID cannot be null")
  @Column(name = "STUDENT_ID")
  UUID studentID;

  /**
   * The Student merge direction code.
   */
  @NotNull(message = "studentMergeDirectionCode cannot be null")
  @Column(name = "STUDENT_MERGE_DIRECTION_CODE")
  String studentMergeDirectionCode;

  /**
   * The Student merge source code.
   */
  @NotNull(message = "studentMergeSourceCode cannot be null")
  @Column(name = "STUDENT_MERGE_SOURCE_CODE")
  String studentMergeSourceCode;

  /**
   * The Create user.
   */
  @Column(name = "CREATE_USER", updatable = false)
  String createUser;

  /**
   * The Create date.
   */
  @Column(name = "CREATE_DATE", updatable = false)
  @PastOrPresent
  LocalDateTime createDate;

  /**
   * The Update user.
   */
  @Column(name = "UPDATE_USER")
  String updateUser;

  /**
   * The Update date.
   */
  @Column(name = "UPDATE_DATE")
  @PastOrPresent
  LocalDateTime updateDate;

  /**
   * The Merge student id.
   */
  @NotNull(message = "mergeStudentID cannot be null")
  @Column(name = "MERGE_STUDENT_ID")
  UUID mergeStudentID;
}
