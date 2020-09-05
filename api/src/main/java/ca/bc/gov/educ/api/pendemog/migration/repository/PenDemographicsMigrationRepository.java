package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * The interface Pen demographics repository.
 */
@Repository
public interface PenDemographicsMigrationRepository extends CrudRepository<PenDemographicsEntity, String> {
  /**
   * Find by stud no optional.
   *
   * @param pen the pen
   * @return the optional
   */
  Optional<PenDemographicsEntity> findByStudNo(String pen);

  List<PenDemographicsEntity> findByStudNoLike(String studNo);

  List<PenDemographicsEntity> findByStudSurnameLike(String studSurnameStartingLetter);
}
