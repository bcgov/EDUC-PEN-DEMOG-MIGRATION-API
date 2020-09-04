package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentMergeEntity;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface StudentMergeRepository extends CrudRepository<StudentMergeEntity, UUID> {
  List<StudentMergeEntity> findStudentMergeEntityByStudentID(UUID studentID);
}
