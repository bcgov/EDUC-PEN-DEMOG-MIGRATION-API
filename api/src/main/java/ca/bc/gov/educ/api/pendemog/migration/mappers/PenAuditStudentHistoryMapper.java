package ca.bc.gov.educ.api.pendemog.migration.mappers;

import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import org.mapstruct.DecoratedWith;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper(uses = StringMapper.class)
@DecoratedWith(PenAuditDecorator.class)
public interface PenAuditStudentHistoryMapper {
  PenAuditStudentHistoryMapper mapper = Mappers.getMapper(PenAuditStudentHistoryMapper.class);

  @Mapping(target = "updateDate", ignore = true)
  @Mapping(target = "createDate", ignore = true)
  @Mapping(target = "dob", ignore = true)
  @Mapping(target = "studentID", ignore = true)
  @Mapping(target = "studentHistoryID", ignore = true)
  @Mapping(target = "memo", ignore = true)
  @Mapping(target = "historyActivityCode", ignore = true)
  @Mapping(target = "emailVerified", constant = "N")
  @Mapping(target = "email", ignore = true)
  @Mapping(target = "deceasedDate", ignore = true)
  StudentHistoryEntity toStudentHistory(PenAuditEntity penAuditEntity);
}
