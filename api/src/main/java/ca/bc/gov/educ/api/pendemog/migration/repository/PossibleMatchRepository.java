package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PossibleMatchEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PossibleMatchRepository extends CrudRepository<PossibleMatchEntity, UUID> {
  List<PossibleMatchEntity> findPossibleMatchEntityByStudentID(UUID studentID);

  Optional<PossibleMatchEntity> findByStudentIDAndMatchedStudentID(UUID studentID, UUID matchedStudentID);
}
