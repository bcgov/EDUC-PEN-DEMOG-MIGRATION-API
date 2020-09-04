package ca.bc.gov.educ.api.pendemog.migration.controller;

import ca.bc.gov.educ.api.pendemog.migration.endpoint.PenDemographicsMigrationEndpoint;
import ca.bc.gov.educ.api.pendemog.migration.service.PenDemographicsMigrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@Slf4j
public class PenDemographicsMigrationController implements PenDemographicsMigrationEndpoint, Closeable {

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final PenDemographicsMigrationService penDemographicsMigrationService;

  @Autowired
  public PenDemographicsMigrationController(PenDemographicsMigrationService penDemographicsMigrationService) {
    this.penDemographicsMigrationService = penDemographicsMigrationService;
  }

  @Override
  public ResponseEntity<Void> kickOffMigrationProcess(String startFromSurnameLike) {
    var startFromLetter = startFromSurnameLike == null ? "AA" : startFromSurnameLike;
    executorService.execute(() -> penDemographicsMigrationService.processDataMigration(startFromLetter));
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<Void> kickOffMergesMigrationProcess() {
    executorService.execute(penDemographicsMigrationService::processMigrationOfMerges);
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<Void> kickOffTwinsMigrationProcess() {
    executorService.execute(penDemographicsMigrationService::processMigrationOfTwins);
    return ResponseEntity.noContent().build();
  }

  @Override
  public void close() {
    if (!executorService.isShutdown()) {
      executorService.shutdown();
    }
  }
}
