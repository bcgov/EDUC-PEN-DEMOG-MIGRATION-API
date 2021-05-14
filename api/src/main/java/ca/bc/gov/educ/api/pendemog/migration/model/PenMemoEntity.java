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

/**
 * The type Pen Memo entity.
 */
@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "API_STUDENT.RDB_PEN_MEMO")
public class PenMemoEntity {

  @Id
  @Column(name = "STUD_NO")
  private String studNo;

  @Column(name = "MEMO")
  private String memo;

}
