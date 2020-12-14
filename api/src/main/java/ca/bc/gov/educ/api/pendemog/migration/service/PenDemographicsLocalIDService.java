package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.properties.ApplicationProperties;
import ca.bc.gov.educ.api.pendemog.migration.repository.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * The type Pen demographics migration service.
 */
@Component
@Slf4j
public class PenDemographicsLocalIDService implements Closeable {

  private final ExecutorService executorService;
  @Getter(AccessLevel.PRIVATE)
  private final PenDemographicsMigrationRepository penDemographicsMigrationRepository;

  private final PENDemogPersistenceService penDemogPersistenceService;

  private final Set<String> studNoSet = new HashSet<>();

  @PostConstruct
  public void init() {
    for (var i = 1000; i < 2000; i++) {
      studNoSet.add("" + i);
    }
    studNoSet.add("2");
    studNoSet.add("3");
    studNoSet.add("4");
    studNoSet.add("5");
    studNoSet.add("6");
    studNoSet.add("7");
    for (var i = 8000; i < 10000; i++) {
      studNoSet.add("" + i);
    }

    log.info("init method completed.");
  }

  @Autowired
  public PenDemographicsLocalIDService(ApplicationProperties applicationProperties, final PenDemographicsMigrationRepository penDemographicsMigrationRepository, final PENDemogPersistenceService penDemogPersistenceService) {
    this.penDemographicsMigrationRepository = penDemographicsMigrationRepository;
    this.penDemogPersistenceService = penDemogPersistenceService;
    executorService = Executors.newFixedThreadPool(applicationProperties.getQueryThreads());
  }


  /**
   * Process data migration.
   */
  public void processLocalIDs() {
    processDemogDataMigration();
  }


  private void processDemogDataMigration() {
    List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (String studNo : studNoSet) {
      final Callable<Boolean> callable = () -> processDemog(studNo);
      futures.add(executorService.submit(callable));
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      for (var future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          log.warn("Error waiting for result", e);
        }
      }
    }
    log.info("All pen demog records have been processed, moving to next phase");
  }

  private Boolean processDemog(String studNoLike) {
    log.debug("Now Processing studNo starting with :: {}", studNoLike);
    List<PenDemographicsEntity> penDemographicsEntities = getPenDemographicsMigrationRepository().findByStudNoLike(studNoLike + "%");
    if (!penDemographicsEntities.isEmpty()) {
      log.debug("Found {} records from pen demog for Stud No :: {}", penDemographicsEntities.size(), studNoLike);
      for(PenDemographicsEntity demog: penDemographicsEntities){
        penDemogPersistenceService.savePENDemog(demog, RandomStringUtils.randomAlphanumeric(RandomUtils.nextInt(8,12)).toUpperCase());
      }

      return true;
    } else {
      log.debug("No Records found for Stud No like :: {} in PEN_DEMOG so skipped.", studNoLike);
      log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
    }

    return true;
  }


  @Override
  public void close() {
    if (!this.executorService.isShutdown()) {
      this.executorService.shutdown();
    }

  }
}
