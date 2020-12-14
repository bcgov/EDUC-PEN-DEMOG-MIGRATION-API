package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.PenDemographicsMigrationRepository;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
public class PENDemogPersistenceService {
  private final PenDemographicsMigrationRepository penDemogRepository;

  @Autowired
  public PENDemogPersistenceService(PenDemographicsMigrationRepository penDemogRepository) {
    this.penDemogRepository = penDemogRepository;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 10, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void savePENDemogs(List<PenDemographicsEntity> penDemogEntities) {
    if (penDemogEntities.size() > 1000) {
      List<List<PenDemographicsEntity>> subSets = Lists.partition(penDemogEntities, 1000);
      log.info("created subset of {} student entities", subSets.size());
      subSets.forEach(penDemogRepository::saveAll);
    } else {
      penDemogRepository.saveAll(penDemogEntities);
    }
  }
}
