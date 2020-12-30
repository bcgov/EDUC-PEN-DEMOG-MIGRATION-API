package ca.bc.gov.educ.api.pendemog.migration.struct;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RowFilter {
  int high;
  int low;
}
