package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentEntity;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface StudentRepository extends CrudRepository<StudentEntity, UUID>, JpaSpecificationExecutor<StudentEntity> {
  Optional<StudentEntity> findStudentEntityByPen(String pen);

  List<StudentEntity> findByLegalLastNameLike(String legalLastName);

}
