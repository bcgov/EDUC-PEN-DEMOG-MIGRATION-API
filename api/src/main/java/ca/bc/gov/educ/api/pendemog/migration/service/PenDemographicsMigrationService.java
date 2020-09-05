package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.*;
import ca.bc.gov.educ.api.pendemog.migration.repository.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The type Pen demographics migration service.
 */
@Component
@Slf4j
public class PenDemographicsMigrationService implements Closeable {

  private final ExecutorService executorService = Executors.newFixedThreadPool(100);
  private final ExecutorService queryExecutors = Executors.newFixedThreadPool(50);
  @Getter(AccessLevel.PRIVATE)
  private final PenDemographicsMigrationRepository penDemographicsMigrationRepository;

  @Getter(AccessLevel.PRIVATE)
  private final StudentRepository studentRepository;

  @Getter(AccessLevel.PRIVATE)
  private final StudentMergeRepository studentMergeRepository;

  @Getter(AccessLevel.PRIVATE)
  private final PenMergeRepository penMergeRepository;
  @Getter(AccessLevel.PRIVATE)
  private final PenTwinRepository penTwinRepository;
  @Getter(AccessLevel.PRIVATE)
  private final StudentTwinRepository studentTwinRepository;

  private final StudentService studentService;

  private final Set<String> studNoSet = new HashSet<>();

  @PostConstruct
  public void init() {
    for (var i = 10000; i < 100000; i++) {
      studNoSet.add("" + i);
    }
    log.info("init method completed.");
  }

  @Autowired
  public PenDemographicsMigrationService(final PenDemographicsMigrationRepository penDemographicsMigrationRepository, StudentRepository studentRepository, StudentMergeRepository studentMergeRepository, PenMergeRepository penMergeRepository, PenTwinRepository penTwinRepository, StudentTwinRepository studentTwinRepository, StudentService studentService) {
    this.penDemographicsMigrationRepository = penDemographicsMigrationRepository;
    this.studentRepository = studentRepository;
    this.studentMergeRepository = studentMergeRepository;
    this.penMergeRepository = penMergeRepository;
    this.penTwinRepository = penTwinRepository;
    this.studentTwinRepository = studentTwinRepository;
    this.studentService = studentService;
  }


  /**
   * Process data migration.
   *
   * @param startLetter the start letter
   */
  public void processDataMigration(String startLetter) {
    processDemogDataMigration(startLetter);
    processMigrationOfTwins();
    processMigrationOfMerges();
  }


  private void processDemogDataMigration(String startFromStudNoLike) {
    log.info("starting data migration from studNo :: {}", startFromStudNoLike);
    List<Future<List<Future<Boolean>>>> futures = new CopyOnWriteArrayList<>();
    boolean isProcessingTillCurrentAlphabetDone = true;
    for (String studNo : studNoSet) {
      if (isProcessingTillCurrentAlphabetDone && studNo.equalsIgnoreCase(startFromStudNoLike)) {
        isProcessingTillCurrentAlphabetDone = false;
      }
      if (!isProcessingTillCurrentAlphabetDone) {
        final Callable<List<Future<Boolean>>> callable = () -> processDemog(studNo);
        futures.add(queryExecutors.submit(callable));
      }
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      for (var future : futures) {
        try {
          val innerFutures  = future.get();
          if(innerFutures != null && !innerFutures.isEmpty()){
            for (var innerFuture : innerFutures)
              try {
                innerFuture.get();
              } catch (ExecutionException | InterruptedException e) {
                log.warn("Error waiting for result", e);
              }
          }else {
            log.info("no inner future for this one.");
          }

        } catch (InterruptedException | ExecutionException e) {
          log.warn("Error waiting for result", e);
        }
      }
    }
    log.info("All pen demog records have been processed, moving to next phase");
  }

  private List<Future<Boolean>> processDemog(String studNoLike) {
    List<Future<Boolean>> futures = new ArrayList<>();
    log.info("Now Processing studNo starting with :: {}", studNoLike);
    List<PenDemographicsEntity> penDemographicsEntities = getPenDemographicsMigrationRepository().findByStudNoLike(studNoLike + "%");
    if (!penDemographicsEntities.isEmpty()) {
      log.info("Found {} records from pen demog for Stud No :: {}", penDemographicsEntities.size(), studNoLike);
      List<StudentEntity> studentEntities = getStudentRepository().findByPenLike(studNoLike + "%");
      log.info("Found {} records from student for pen :: {}", studentEntities.size(), studNoLike);
      List<PenDemographicsEntity> penDemographicsEntitiesToBeProcessed = penDemographicsEntities.stream().filter(penDemographicsEntity ->
          studentEntities.stream().allMatch(studentEntity -> (!penDemographicsEntity.getStudNo().trim().equals(studentEntity.getPen())))).collect(Collectors.toList());
      log.info("Found {} records for studNo starting with {} which are not processed and now processing.", penDemographicsEntitiesToBeProcessed.size(), studNoLike);
      if (!penDemographicsEntitiesToBeProcessed.isEmpty()) {
        final Callable<Boolean> callable = () -> studentService.processDemographicsEntities(penDemographicsEntitiesToBeProcessed, studNoLike);
        futures.add(executorService.submit(callable));
      }
    } else {
      log.info("No Records found for Stud No like :: {} in PEN_DEMOG so skipped.", studNoLike);
    }

    return futures;
  }

  public void processMigrationOfMerges() {
    log.info("Starting data migration of Merges");
    List<StudentMergeEntity> mergeFromEntities = new ArrayList<>();
    List<StudentMergeEntity> mergeTOEntities = new ArrayList<>();
    var penMerges = penMergeRepository.findAll();
    if (!penMerges.isEmpty()) {
      createMergedRecords(penMerges, mergeFromEntities, mergeTOEntities);
      studentMergeRepository.saveAll(mergeFromEntities);
      studentMergeRepository.saveAll(mergeTOEntities);
    }
    log.info("finished data migration of Merges, persisted {} merge from  records and {} merge to records to DB", mergeFromEntities.size(), mergeTOEntities.size());
  }

  private void createMergedRecords(List<PenMergesEntity> penMerges, List<StudentMergeEntity> mergeFromEntities, List<StudentMergeEntity> mergeTOEntities) {

    Map<String, List<String>> mergeFromMap = new HashMap<>();
    for (var penMerge : penMerges) {
      if (mergeFromMap.containsKey(penMerge.getStudNo().trim())) {
        List<String> penNumbers = mergeFromMap.get(penMerge.getStudNo().trim());
        penNumbers.add(penMerge.getStudNo().trim());
        mergeFromMap.put(penMerge.getStudNo().trim(), penNumbers);
      } else {
        List<String> penNumbers = new ArrayList<>();
        penNumbers.add(penMerge.getStudNo().trim());
        mergeFromMap.put(penMerge.getStudNo().trim(), penNumbers);
      }
    }
    mergeFromMap.forEach((truePen, penList) -> {
      var originalStudent = studentRepository.findStudentEntityByPen(truePen);
      penList.forEach(penNumber -> {
        var mergedStudent = studentRepository.findStudentEntityByPen(penNumber);
        if (originalStudent.isPresent() && mergedStudent.isPresent()) {
          StudentMergeEntity mergeFromEntity = new StudentMergeEntity();
          mergeFromEntity.setStudentMergeSourceCode("MINISTRY"); // mapped from MERGE_TO_USER_BANE of PEN_DEMOG, default value, currently it is
          mergeFromEntity.setStudentMergeDirectionCode("FROM");
          mergeFromEntity.setStudentID(originalStudent.get().getStudentID()); // TO
          mergeFromEntity.setMergeStudent(mergedStudent.get()); // FROM
          mergeFromEntity.setCreateDate(LocalDateTime.now());
          mergeFromEntity.setUpdateDate(LocalDateTime.now());
          mergeFromEntity.setCreateUser(originalStudent.get().getCreateUser());
          mergeFromEntity.setUpdateUser(originalStudent.get().getUpdateUser());
          mergeFromEntities.add(mergeFromEntity);

          StudentMergeEntity mergeTOEntity = new StudentMergeEntity();
          mergeTOEntity.setStudentMergeSourceCode("MINISTRY");
          mergeTOEntity.setStudentMergeDirectionCode("TO");
          mergeTOEntity.setStudentID(mergedStudent.get().getStudentID()); // FROM
          mergeTOEntity.setMergeStudent(originalStudent.get()); // TO
          mergeTOEntity.setCreateDate(LocalDateTime.now());
          mergeTOEntity.setUpdateDate(LocalDateTime.now());
          mergeTOEntity.setCreateUser(originalStudent.get().getCreateUser());
          mergeTOEntity.setUpdateUser(originalStudent.get().getUpdateUser());
          mergeTOEntities.add(mergeTOEntity);
        } else {
          log.error("student entity not found for true pen and pen :: {} :: {}", truePen, penNumber);
        }
      });
    });
  }

  public void processMigrationOfTwins() {
    //TWIN_REASON_CODE=PENMATCH
    log.info("Starting data migration of Twins");
    List<StudentTwinEntity> twinEntities = new ArrayList<>();
    var penTwins = getPenTwinRepository().findAll();
    log.info("found {} records .", penTwins.size());
    penTwins.parallelStream().forEach(createTwinEntities(twinEntities));
    if (!twinEntities.isEmpty()) {
      log.info("created {} twinned entities", twinEntities.size());
      getStudentTwinRepository().saveAll(twinEntities);
      log.info("saved all twinned entities {}", twinEntities.size());
    }
  }

  private Consumer<PenTwinsEntity> createTwinEntities(List<StudentTwinEntity> twinEntities) {
    return penTwinsEntity -> {
      var studentEntityOptional1 = getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin1().trim());
      var studentEntityOptional2 = getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin2().trim());
      if (studentEntityOptional1.isPresent() && studentEntityOptional2.isPresent()) {
        StudentTwinEntity studentTwinEntity1 = new StudentTwinEntity();
        studentTwinEntity1.setCreateDate(LocalDateTime.now());
        studentTwinEntity1.setUpdateDate(LocalDateTime.now());
        if (penTwinsEntity.getTwinUserId() != null && !"".equalsIgnoreCase(penTwinsEntity.getTwinUserId().trim())) {
          studentTwinEntity1.setCreateUser(penTwinsEntity.getTwinUserId().trim());
          studentTwinEntity1.setUpdateUser(penTwinsEntity.getTwinUserId().trim());
        } else {
          studentTwinEntity1.setCreateUser("PEN_DEMOG_MIGRATION_API");
          studentTwinEntity1.setUpdateUser("PEN_DEMOG_MIGRATION_API");
        }
        studentTwinEntity1.setStudentTwinReasonCode("PENMATCH");
        studentTwinEntity1.setStudentID(studentEntityOptional1.get().getStudentID());
        studentTwinEntity1.setTwinStudent(studentEntityOptional2.get());

       /* StudentTwinEntity studentTwinEntity2 = new StudentTwinEntity();
        studentTwinEntity2.setCreateDate(LocalDateTime.now());
        studentTwinEntity2.setUpdateDate(LocalDateTime.now());
        if (penTwinsEntity.getTwinUserId() != null && !"".equalsIgnoreCase(penTwinsEntity.getTwinUserId().trim())) {
          studentTwinEntity2.setCreateUser(penTwinsEntity.getTwinUserId().trim());
          studentTwinEntity2.setUpdateUser(penTwinsEntity.getTwinUserId().trim());
        } else {
          studentTwinEntity2.setCreateUser("PEN_DEMOG_MIGRATION_API");
          studentTwinEntity2.setUpdateUser("PEN_DEMOG_MIGRATION_API");
        }
        studentTwinEntity2.setStudentTwinReasonCode("PENMATCH");
        studentTwinEntity2.setStudentID(studentEntityOptional2.get().getStudentID());
        studentTwinEntity2.setTwinStudent(studentEntityOptional1.get());*/
        twinEntities.add(studentTwinEntity1);
        //twinEntities.add(studentTwinEntity2);
      } else {
        log.error("Student entity could not be found for twin 1 pen :: {} and twin 2 pen :: {}", penTwinsEntity.getPenTwin1().trim(), penTwinsEntity.getPenTwin2().trim());
      }
    };
  }

  @Override
  public void close() {
    if (!this.executorService.isShutdown()) {
      this.executorService.shutdown();
    }

  }
}
