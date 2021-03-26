package ca.bc.gov.educ.api.pendemog.migration.constants;

import lombok.Getter;

public enum DemogCodes {
  ACCEPTED("A"),
  CONFIRMED("C"),
  FROZEN("F");
  @Getter
  private final String code;

  DemogCodes(final String code) {
    this.code = code;
  }
}
