package ca.bc.gov.educ.api.pendemog.migration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;

import javax.persistence.*;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Immutable
@Table(name = "PEN_TWINS@PENLINK.WORLD")
@IdClass(PenTwinsPK.class)
public class PenTwinsEntity {

  @Id
  @Column(name = "PEN_TWIN1")
  String penTwin1;
  @Id
  @Column(name = "PEN_TWIN2")
  String penTwin2;
  @Column(name = "TWIN_REASON")
  String twinReason;
  @Column(name = "RUN_DATE")
  String runDate;
  @Column(name = "TWIN_DATE")
  String twinDate;
  @Column(name = "TWIN_USER_ID")
  String twinUserId;

  public PenTwinsPK getId() {
    return new PenTwinsPK(
        penTwin1,
        penTwin2
    );
  }

  public void setId(PenTwinsPK id) {
    this.penTwin1 = id.getPenTwin1();
    this.penTwin2 = id.getPenTwin2();
  }
}

