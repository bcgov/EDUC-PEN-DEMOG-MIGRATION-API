package ca.bc.gov.educ.api.pendemog.migration.constants;

import lombok.Getter;

public enum HistoryActivityCode {

  REQ_NEW("REQNEW"),
  USER_NEW("USERNEW"),
  USER_EDIT("USEREDIT");

  @Getter
  private final String code;

  HistoryActivityCode(String code) {
    this.code = code;
  }
}
