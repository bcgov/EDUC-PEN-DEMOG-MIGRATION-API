package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.StudentHistoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface StudentHistoryRepository extends JpaRepository<StudentHistoryEntity, UUID>{
  List<StudentHistoryEntity> findAllByPenLike(String pen);
}
