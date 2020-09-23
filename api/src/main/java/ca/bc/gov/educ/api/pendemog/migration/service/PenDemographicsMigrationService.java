package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.model.*;
import ca.bc.gov.educ.api.pendemog.migration.repository.*;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The type Pen demographics migration service.
 */
@Component
@Slf4j
public class PenDemographicsMigrationService implements Closeable {

  private final ExecutorService executorService = Executors.newFixedThreadPool(100);
  private final ExecutorService queryExecutors = Executors.newFixedThreadPool(48);
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
   */
  @Transactional
  public void processDataMigration() {
    processDemogDataMigration();
    processMigrationOfTwins();
    processMigrationOfMerges();
  }


  private void processDemogDataMigration() {
    List<Future<List<Future<Boolean>>>> futures = new CopyOnWriteArrayList<>();
    for (String studNo : studNoSet) {
      final Callable<List<Future<Boolean>>> callable = () -> processDemog(studNo);
      futures.add(queryExecutors.submit(callable));
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      for (var future : futures) {
        try {
          val innerFutures = future.get();
          if (innerFutures != null && !innerFutures.isEmpty()) {
            for (var innerFuture : innerFutures)
              try {
                innerFuture.get();
              } catch (ExecutionException | InterruptedException e) {
                log.warn("Error waiting for result", e);
              }
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
    log.debug("Now Processing studNo starting with :: {}", studNoLike);
    List<PenDemographicsEntity> penDemographicsEntities = getPenDemographicsMigrationRepository().findByStudNoLike(studNoLike + "%");
    if (!penDemographicsEntities.isEmpty()) {
      log.debug("Found {} records from pen demog for Stud No :: {}", penDemographicsEntities.size(), studNoLike);
      List<StudentEntity> studentEntities = getStudentRepository().findByPenLike(studNoLike + "%");
      log.debug("Found {} records from student for pen :: {}", studentEntities.size(), studNoLike);
      List<PenDemographicsEntity> penDemographicsEntitiesToBeProcessed = penDemographicsEntities.stream().filter(penDemographicsEntity ->
          studentEntities.stream().allMatch(studentEntity -> (!penDemographicsEntity.getStudNo().trim().equals(studentEntity.getPen())))).collect(Collectors.toList());
      if (!penDemographicsEntitiesToBeProcessed.isEmpty()) {
        log.debug("Found {} records for studNo starting with {} which are not processed and now processing.", penDemographicsEntitiesToBeProcessed.size(), studNoLike);
        final Callable<Boolean> callable = () -> studentService.processDemographicsEntities(penDemographicsEntitiesToBeProcessed, studNoLike);
        futures.add(executorService.submit(callable));
      } else {
        log.debug("Nothing to process for :: {} marking complete. total number of records processed :: {}", studNoLike, CounterUtil.processCounter.incrementAndGet());
      }
    } else {
      log.debug("No Records found for Stud No like :: {} in PEN_DEMOG so skipped.", studNoLike);
      log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
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
      List<List<StudentMergeEntity>> mergeFromSubset = Lists.partition(mergeFromEntities, 1000);
      List<List<StudentMergeEntity>> mergeToSubset = Lists.partition(mergeTOEntities, 1000);
      log.info("created subset of {} merge from  entities", mergeFromSubset.size());
      log.info("created subset of {} merge to  entities", mergeToSubset.size());
      mergeFromSubset.parallelStream().forEach(entities -> getStudentMergeRepository().saveAll(entities));
      mergeToSubset.parallelStream().forEach(entities -> getStudentMergeRepository().saveAll(entities));
    }
    log.info("finished data migration of Merges, persisted {} merge from  records and {} merge to records to DB", mergeFromEntities.size(), mergeTOEntities.size());
  }

  private void createMergedRecords(List<PenMergesEntity> penMerges, List<StudentMergeEntity> mergeFromEntities, List<StudentMergeEntity> mergeTOEntities) {
    final AtomicInteger counter = new AtomicInteger();
    Map<String, List<String>> mergeEntitiesMap = new HashMap<>();
    for (var penMerge : penMerges) {
      if (mergeEntitiesMap.containsKey(penMerge.getStudTrueNo().trim())) {
        List<String> penNumbers = mergeEntitiesMap.get(penMerge.getStudTrueNo().trim());
        penNumbers.add(penMerge.getStudNo().trim());
        mergeEntitiesMap.put(penMerge.getStudTrueNo().trim(), penNumbers);
      } else {
        List<String> penNumbers = new ArrayList<>();
        penNumbers.add(penMerge.getStudNo().trim());
        mergeEntitiesMap.put(penMerge.getStudTrueNo().trim(), penNumbers);
      }
    }
    log.info("Total Entries in Merges MAP {}", mergeEntitiesMap.size());
    mergeEntitiesMap.forEach(findAndCreateMergeEntities(mergeFromEntities, mergeTOEntities, counter));
  }

  private BiConsumer<String, List<String>> findAndCreateMergeEntities(List<StudentMergeEntity> mergeFromEntities, List<StudentMergeEntity> mergeTOEntities, AtomicInteger counter) {
    return (truePen, penList) -> {
      var originalStudent = studentRepository.findStudentEntityByPen(truePen);
      penList.parallelStream().forEach(createMergeStudentEntities(mergeFromEntities, mergeTOEntities, counter, truePen, originalStudent));
    };
  }

  private Consumer<String> createMergeStudentEntities(List<StudentMergeEntity> mergeFromEntities, List<StudentMergeEntity> mergeTOEntities, AtomicInteger counter, String truePen, Optional<StudentEntity> originalStudent) {
    return penNumber -> {
      log.info("Index {}, creating merge from and merge to entity for true pen and pen :: {} {}", counter.incrementAndGet(), truePen, penNumber);
      var mergedStudent = studentRepository.findStudentEntityByPen(penNumber);
      if (originalStudent.isPresent() && mergedStudent.isPresent()) {
        StudentMergeEntity mergeFromEntity = createMergeEntity(mergedStudent.get(), originalStudent.get().getStudentID(), "FROM");
        log.debug("Index {}, merge from  entity {}", counter.get(), mergeFromEntity.toString());
        mergeFromEntities.add(mergeFromEntity);

        StudentMergeEntity mergeTOEntity = createMergeEntity(originalStudent.get(), mergedStudent.get().getStudentID(), "TO");
        log.debug("Index {}, merge to  entity {}", counter.get(), mergeTOEntity.toString());
        mergeTOEntities.add(mergeTOEntity);
      } else {
        log.error("Index {}, student entity not found for true pen and pen :: {} :: {}", counter.get(), truePen, penNumber);
      }
    };
  }

  private StudentMergeEntity createMergeEntity(StudentEntity mergeStudent, UUID studentId, String direction) {
    StudentMergeEntity mergeTOEntity = new StudentMergeEntity();
    mergeTOEntity.setStudentMergeSourceCode("MINISTRY");
    mergeTOEntity.setStudentMergeDirectionCode(direction);
    mergeTOEntity.setStudentID(studentId);
    mergeTOEntity.setMergeStudent(mergeStudent);
    mergeTOEntity.setCreateDate(LocalDateTime.now());
    mergeTOEntity.setUpdateDate(LocalDateTime.now());
    mergeTOEntity.setCreateUser(mergeStudent.getCreateUser());
    mergeTOEntity.setUpdateUser(mergeStudent.getUpdateUser());
    return mergeTOEntity;
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
      List<List<StudentTwinEntity>> subSets = Lists.partition(twinEntities, 1000);
      log.info("created subset of {} twinned entities", subSets.size());
      subSets.parallelStream().forEach(studentTwinEntities -> getStudentTwinRepository().saveAll(studentTwinEntities));
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
