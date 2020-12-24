package ca.bc.gov.educ.api.pendemog.migration.controller;

import ca.bc.gov.educ.api.pendemog.migration.endpoint.PenDemographicsMigrationEndpoint;
import ca.bc.gov.educ.api.pendemog.migration.service.PenDemographicsLocalIDService;
import ca.bc.gov.educ.api.pendemog.migration.service.PenDemographicsMigrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@Slf4j
public class PenDemographicsMigrationController implements PenDemographicsMigrationEndpoint, Closeable {
  private final DataSource dataSource;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final PenDemographicsMigrationService penDemographicsMigrationService;
  private final PenDemographicsLocalIDService penDemographicsLocalIDService;

  @Autowired
  public PenDemographicsMigrationController(DataSource dataSource, PenDemographicsMigrationService penDemographicsMigrationService, PenDemographicsLocalIDService penDemographicsLocalIDService) {
    this.dataSource = dataSource;
    this.penDemographicsMigrationService = penDemographicsMigrationService;
    this.penDemographicsLocalIDService = penDemographicsLocalIDService;
  }

  @Override
  public ResponseEntity<Void> kickOffMigrationProcess() {
    executorService.execute(penDemographicsMigrationService::processDataMigration);
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<Void> kickOffLocalIDProcess() {
    executorService.execute(penDemographicsLocalIDService::processLocalIDs);
    return ResponseEntity.noContent().build();
  }

  @Override
  public ResponseEntity<Void> kickOffAuditDataMigrationProcess() {
    executorService.execute(penDemographicsMigrationService::processDemogAuditDataMigration);
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

  public ResponseEntity<Void> testQuery(String sql) {
    try(var resultSet = dataSource.getConnection().prepareStatement(sql).executeQuery()) {
      log.info(sql);
      while (resultSet.next()) {
        log.info("result is :: {}", resultSet.getObject(1));
      }
    } catch (final Exception e) {
      log.error("Exception :: {}", e, e);
    }
    return ResponseEntity.noContent().build();
  }

  @Override
  public void close() {
    if (!executorService.isShutdown()) {
      executorService.shutdown();
    }
  }
}
