package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PenMergePK;
import ca.bc.gov.educ.api.pendemog.migration.model.PenMergesEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PenMergeRepository extends CrudRepository<PenMergesEntity, PenMergePK> {
  List<PenMergesEntity> findAll();
}
