package ca.bc.gov.educ.api.pendemog.migration.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PenTwinsPK implements Serializable {
  private String penTwin1;
  private String penTwin2;

}
