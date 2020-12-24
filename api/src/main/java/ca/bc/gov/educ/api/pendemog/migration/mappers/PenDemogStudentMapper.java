package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper(uses = StringMapper.class)
public interface PenDemogStudentMapper {
  PenDemogStudentMapper mapper = Mappers.getMapper(PenDemogStudentMapper.class);

  @Mapping(target = "usualMiddleNames", source = "usualMiddle")
  @Mapping(target = "usualLastName", source = "usualSurname")
  @Mapping(target = "usualFirstName", source = "usualGiven")
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
  @Mapping(target = "localID", source = "localID")
  @Mapping(target = "emailVerified", constant = "N")
  @Mapping(target = "email", ignore = true)
  @Mapping(target = "dob", ignore = true)
  @Mapping(target = "deceasedDate", ignore = true)
  @Mapping(target = "createUser", source = "createUserName")
  @Mapping(target = "updateUser", source = "updateUserName")
  StudentEntity toStudent(PenDemographicsEntity penDemographicsEntity);
}
