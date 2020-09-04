package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper(uses = StringMapper.class)
public interface PenDemogStudentMapper {
  PenDemogStudentMapper mapper = Mappers.getMapper(PenDemogStudentMapper.class);

  @Mapping(target = "updateDate", ignore = true)
  @Mapping(target = "createDate", ignore = true)
  @Mapping(target = "usualMiddleNames", source = "usualMiddle")
  @Mapping(target = "usualLastName", source = "usualSurname")
  @Mapping(target = "usualFirstName", source = "usualGiven")
  @Mapping(target = "updateUser", constant = "PEN_DEMOG_MIGRATION_API")
  @Mapping(target = "studentID", ignore = true)
  @Mapping(target = "statusCode", source = "studStatus")
  @Mapping(target = "sexCode", source = "studSex")
  @Mapping(target = "pen", source = "studNo")
  @Mapping(target = "memo", ignore = true)
  @Mapping(target = "legalMiddleNames", source = "studMiddle")
  @Mapping(target = "legalLastName", source = "studSurname")
  @Mapping(target = "legalFirstName", source = "studGiven")
  @Mapping(target = "gradeCode", source = "grade")
  @Mapping(target = "genderCode", source = "studSex")
  @Mapping(target = "emailVerified", constant = "N")
  @Mapping(target = "email", ignore = true)
  @Mapping(target = "dob", ignore = true)
  @Mapping(target = "deceasedDate", ignore = true)
  @Mapping(target = "createUser", constant = "PEN_DEMOG_MIGRATION_API")
  StudentEntity toStudent(PenDemographicsEntity penDemographicsEntity);
}
