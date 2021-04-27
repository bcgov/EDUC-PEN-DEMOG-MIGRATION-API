package ca.bc.gov.educ.api.pendemog.migration.constants;

import lombok.Getter;

/**
 * The enum Student merge source codes.
 */
public enum StudentMergeSourceCodes {
  SCHOOL("SCHOOL", "SCHOOL"),

  STUDENT("STUDENT", "STUDENT"),

  BM("BM", "BM"),

  DE("DE", "DE"),

  DR("DR", "DR"),

  FR("FR", "FR"),

  IF("IF", "IF"),

  MA("MA", "MA"),

  MI("MI", "MINISTRY"),

  PR("PR", "PR"),

  SC("SC", "SC"),

  SD("SD", "SD"),

  SR("SR", "SR"),

  TX("TX", "TX");

  @Getter
  private final String oldCode;
  @Getter
  private final String prrCode;

  StudentMergeSourceCodes(String oldCode, String prrCode) {
    this.oldCode = oldCode;
    this.prrCode = prrCode;
  }
}
