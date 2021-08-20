package ca.bc.gov.educ.api.pendemog.migration.endpoint;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/")
public interface PenDemographicsMigrationEndpoint {

  @GetMapping
  ResponseEntity<Void> kickOffMigrationProcess();

  @GetMapping("/localids")
  ResponseEntity<Void> kickOffLocalIDProcess();

  @GetMapping("/audit-and-after")
  ResponseEntity<Void> kickOffAuditAndAfter();

  @GetMapping("/demog")
  ResponseEntity<Void> kickOffDemogDataMigrationProcess();

  @GetMapping("/audit")
  ResponseEntity<Void> kickOffAuditDataMigrationProcess();

  @GetMapping("/merges")
  ResponseEntity<Void> kickOffMergesMigrationProcess();

  @GetMapping("/twins")
  ResponseEntity<Void> kickOffTwinsMigrationProcess();

  @GetMapping("/memos")
  ResponseEntity<Void> kickOffMemoMigrationProcess();

  @GetMapping("/query")
  ResponseEntity<Void> testQuery(@RequestParam(name = "query") String query);
}
