package ca.bc.gov.educ.api.pendemog.migration.constants;

import lombok.Getter;

public enum GradeCodes {
  ONE("01"),

  TWO("02"),

  THREE("03"),

  FOUR("04"),

  FIVE("05"),

  SIX("06"),

  SEVEN("07"),

  EIGHT("08"),

  NINE("09"),

  TEN("10"),

  ELEVEN("11"),

  TWELVE("12"),

  EL("EL"),

  GA("GA"),

  KF("KF"),

  SU("SU"),

  EU("EU"),

  HS("HS"),

  KH("KH");

  @Getter
  private final String code;

  GradeCodes(String code) {
    this.code = code;
  }
}
