package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PenTwinsEntity;
import ca.bc.gov.educ.api.pendemog.migration.model.PenTwinsPK;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PenTwinRepository extends CrudRepository<PenTwinsEntity, PenTwinsPK> {
  List<PenTwinsEntity> findAll();
}
