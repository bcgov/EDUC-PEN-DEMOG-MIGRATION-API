package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentTwinEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface StudentTwinRepository extends CrudRepository<StudentTwinEntity, UUID> {
  List<StudentTwinEntity> findStudentTwinEntityByStudentID(UUID studentID);
}
