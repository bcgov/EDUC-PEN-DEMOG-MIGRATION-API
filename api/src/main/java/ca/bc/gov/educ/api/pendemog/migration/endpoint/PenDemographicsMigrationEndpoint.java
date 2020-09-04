package ca.bc.gov.educ.api.pendemog.migration.endpoint;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/")
public interface PenDemographicsMigrationEndpoint {

  @GetMapping
  ResponseEntity<Void> kickOffMigrationProcess(@RequestParam(name = "startFromSurnameLike", required = false) String startFromSurnameLike);

  @GetMapping("/merges")
  ResponseEntity<Void> kickOffMergesMigrationProcess();

  @GetMapping("/twins")
  ResponseEntity<Void> kickOffTwinsMigrationProcess();

}
