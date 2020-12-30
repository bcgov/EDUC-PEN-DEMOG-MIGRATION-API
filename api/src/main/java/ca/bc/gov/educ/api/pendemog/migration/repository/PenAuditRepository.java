package ca.bc.gov.educ.api.pendemog.migration.repository;

import ca.bc.gov.educ.api.pendemog.migration.model.PenAuditEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PenAuditRepository extends JpaRepository<PenAuditEntity, String> {
  List<PenAuditEntity> findByPenLike(String pen);

  List<PenAuditEntity> findByPen(String pen);
}
