package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PenMemoEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.PenMergePK;
import ca.bc.gov.educ.api.pendemog.migration.model.PenMergesEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PenMemoRepository extends JpaRepository<PenMemoEntity, String> {
  List<PenMemoEntity> findAllByStudNo(String studNo);
}
