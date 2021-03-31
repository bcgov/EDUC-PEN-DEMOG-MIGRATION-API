package ca.bc.gov.educ.api.pendemog.migration.constants;

import lombok.Getter;

/**
 * The enum Student merge source codes.
 */
public enum StudentMergeSourceCodes {
  /**
   * BM Merge Code
   */
  BM("BM"),
  /**
   * DE Merge Code
   */
  DE("DE"),
  /**
   * DR Merge Code
   */
  DR("DR"),
  /**
   * IF Merge Code
   */
  IF("IF"),
  /**
   * MA Merge Code
   */
  MA("MA"),
  /**
   * Ministry Merge Code
   */
  MI("MI"),
  /**
   * PR Merge Code
   */
  PR("PR"),
  /**
   * SC Merge Code
   */
  SC("SC"),
  /**
   * SD Merge Code
   */
  SD("SD"),
  /**
   * SR Merge Code
   */
  SR("SR"),
  /**
   * TX Merge Code
   */
  TX("TX");

  /**
   * The Code.
   */
  @Getter
  private final String code;

  /**
   * Instantiates a new student merge source code.
   *
   * @param code the code
   */
  StudentMergeSourceCodes(final String code) {
    this.code = code;
  }
}
