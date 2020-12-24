package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.model.*;
import ca.bc.gov.educ.api.pendemog.migration.properties.ApplicationProperties;
import ca.bc.gov.educ.api.pendemog.migration.repository.*;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

  private final ExecutorService executorService;
  @Getter(AccessLevel.PRIVATE)
  private final PenDemographicsMigrationRepository penDemographicsMigrationRepository;

  @Getter(AccessLevel.PRIVATE)
  private final PenAuditRepository penAuditRepository;

  @Getter(AccessLevel.PRIVATE)
  private final StudentRepository studentRepository;

  @Getter(AccessLevel.PRIVATE)
  private final StudentHistoryRepository studentHistoryRepository;

  @Getter(AccessLevel.PRIVATE)
  private final StudentMergeRepository studentMergeRepository;

  @Getter(AccessLevel.PRIVATE)
  private final PenMergeRepository penMergeRepository;
  @Getter(AccessLevel.PRIVATE)
  private final PenTwinRepository penTwinRepository;
  @Getter(AccessLevel.PRIVATE)
  private final StudentTwinRepository studentTwinRepository;
  @Getter(AccessLevel.PRIVATE)
  private final StudentTwinService studentTwinService;
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
  public PenDemographicsMigrationService(ApplicationProperties applicationProperties, final PenDemographicsMigrationRepository penDemographicsMigrationRepository, PenAuditRepository penAuditRepository, StudentRepository studentRepository, StudentHistoryRepository studentHistoryRepository, StudentMergeRepository studentMergeRepository, PenMergeRepository penMergeRepository, PenTwinRepository penTwinRepository, StudentTwinRepository studentTwinRepository, StudentTwinService studentTwinService, StudentService studentService) {
    this.penDemographicsMigrationRepository = penDemographicsMigrationRepository;
    this.penAuditRepository = penAuditRepository;
    this.studentRepository = studentRepository;
    this.studentHistoryRepository = studentHistoryRepository;
    this.studentMergeRepository = studentMergeRepository;
    this.penMergeRepository = penMergeRepository;
    this.penTwinRepository = penTwinRepository;
    this.studentTwinRepository = studentTwinRepository;
    this.studentTwinService = studentTwinService;
    this.studentService = studentService;
    executorService = Executors.newFixedThreadPool(applicationProperties.getQueryThreads());
  }


  /**
   * Process data migration.
   */
  public void processDataMigration() {
    processDemogDataMigration();
    processDemogAuditDataMigration();
    processMigrationOfTwins();
    processMigrationOfMerges();
  }

  public void processDemogAuditDataMigration() {
    List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (String studNo : studNoSet) {
      final Callable<Boolean> callable = () -> processDemogAudit(studNo);
      futures.add(executorService.submit(callable));
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      for (var future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          Thread.currentThread().interrupt();
          log.warn("Error waiting for result", e);
        }
      }
    }
    log.info("All pen demog audit records have been processed.");
  }

  private Boolean processDemogAudit(String penLike) {
    log.debug("Now Processing penLike starting with :: {}", penLike);
    List<PenAuditEntity> filteredAuditEntities = new CopyOnWriteArrayList<>();
    var penAuditEntities = new CopyOnWriteArrayList<>(getPenAuditRepository().findByPenLike(penLike + "%"));
    if (!penAuditEntities.isEmpty()) {
      log.debug("Found {} records from penLike demog audit for Stud No :: {}", penAuditEntities.size(), penLike);
      List<StudentHistoryEntity> studentHistoryEntities = new CopyOnWriteArrayList<>(getStudentHistoryRepository().findAllByPenLike(penLike + "%"));
      log.debug("Found {} records from student history for penLike :: {}", studentHistoryEntities.size(), penLike);
      if (!studentHistoryEntities.isEmpty()) {
        for (var studentHistory : studentHistoryEntities) {
          for (var penAuditEntity : penAuditEntities) {
            if (StringUtils.equals(penAuditEntity.getPen(), studentHistory.getPen())
                && penAuditEntity.getDob().isEqual(studentHistory.getDob())
                && penAuditEntity.getActivityDate().isEqual(studentHistory.getCreateDate())
                && StringUtils.equals(penAuditEntity.getCreateUser(),studentHistory.getCreateUser())) {
              penAuditEntities.remove(penAuditEntity);
              studentHistoryEntities.remove(studentHistory);
            }else {
              filteredAuditEntities.add(penAuditEntity);
            }
          }
        }
      } else {
        filteredAuditEntities = penAuditEntities;
      }
      if (!filteredAuditEntities.isEmpty()) {
        log.debug("Found {} records for studNo starting with {} which are not processed and now processing.", filteredAuditEntities.size(), penLike);
        return studentService.processDemographicsAuditEntities(filteredAuditEntities, penLike);
      } else {
        log.info("Nothing to process for :: {} marking complete. total number of history records processed :: {}", penLike, CounterUtil.historyProcessCounter.incrementAndGet());
      }
    } else {
      log.debug("No Records found for Stud No like :: {} in PEN_AUDIT so skipped.", penLike);
      log.info("total number of history records processed :: {}", CounterUtil.historyProcessCounter.incrementAndGet());
    }

    return true;
  }


  public void processDemogDataMigration() {
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
          Thread.currentThread().interrupt();
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
      List<StudentEntity> studentEntities = getStudentRepository().findByPenLike(studNoLike + "%");
      log.debug("Found {} records from student for pen :: {}", studentEntities.size(), studNoLike);
      List<PenDemographicsEntity> penDemographicsEntitiesToBeProcessed = penDemographicsEntities.stream().filter(penDemographicsEntity ->
          studentEntities.stream().allMatch(studentEntity -> (!penDemographicsEntity.getStudNo().trim().equals(studentEntity.getPen())))).collect(Collectors.toList());
      if (!penDemographicsEntitiesToBeProcessed.isEmpty()) {
        log.debug("Found {} records for studNo starting with {} which are not processed and now processing.", penDemographicsEntitiesToBeProcessed.size(), studNoLike);
        return studentService.processDemographicsEntities(penDemographicsEntitiesToBeProcessed, studNoLike);
      } else {
        log.debug("Nothing to process for :: {} marking complete. total number of records processed :: {}", studNoLike, CounterUtil.processCounter.incrementAndGet());
      }
    } else {
      log.debug("No Records found for Stud No like :: {} in PEN_DEMOG so skipped.", studNoLike);
      log.info("total number of records processed :: {}", CounterUtil.processCounter.incrementAndGet());
    }

    return true;
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
      mergeFromSubset.forEach(getStudentMergeRepository()::saveAll);
      mergeToSubset.forEach(getStudentMergeRepository()::saveAll);
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
    log.info("Starting data migration of Twins");
    List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (String studNo : studNoSet) {
      final Callable<Boolean> callable = () -> processTwinForPenLike(studNo);
      futures.add(executorService.submit(callable));
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      int index = 1;
      for (var future : futures) {
        try {
          future.get();
          log.info("Total completed is :: {}", index++);
        } catch (InterruptedException | ExecutionException e) {
          log.warn("Error waiting for result", e);
        }
      }
    }
    log.info("All pen twin records have been processed, moving to next phase");
    //TWIN_REASON_CODE=PENMATCH

  }

  private Boolean processTwinForPenLike(String penLike) {

    List<StudentTwinEntity> twinEntities = new CopyOnWriteArrayList<>();
    var penTwins = getPenTwinRepository().findByPenTwin1Like(penLike + "%");
    var studentTwins = getStudentRepository().findByPenLike(penLike + "%");
    var studentTwinMap = studentTwins.stream()
        .collect(Collectors.toMap(StudentEntity::getPen, studentEntity -> studentEntity));
    log.info("found {} records .", penTwins.size());
    if (!penTwins.isEmpty()) {
      penTwins.forEach(penTwinsEntity -> {
        StudentEntity student1;
        StudentEntity student2;
        var studentEntity1 = Optional.ofNullable(studentTwinMap.get(penTwinsEntity.getPenTwin1().trim()));
        if (studentEntity1.isEmpty()) {
          studentEntity1 = getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin1().trim());
        }
        if (studentEntity1.isPresent()) {
          var studentEntity2 = Optional.ofNullable(studentTwinMap.get(penTwinsEntity.getPenTwin2().trim()));
          if (studentEntity2.isEmpty()) {
            studentEntity2 = getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin2().trim());
          }
          if (studentEntity2.isPresent()) {
            student1 = studentEntity1.get();
            student2 = studentEntity2.get();
            Optional<StudentTwinEntity> dbEntity = getStudentTwinRepository().findByStudentIDAndTwinStudent(student1.getStudentID(), student2);
            if (dbEntity.isEmpty()) {
              StudentTwinEntity studentTwinEntity = new StudentTwinEntity();
              studentTwinEntity.setCreateDate(LocalDateTime.now());
              studentTwinEntity.setUpdateDate(LocalDateTime.now());
              if (penTwinsEntity.getTwinUserId() != null && !"".equalsIgnoreCase(penTwinsEntity.getTwinUserId().trim())) {
                studentTwinEntity.setCreateUser(penTwinsEntity.getTwinUserId().trim());
                studentTwinEntity.setUpdateUser(penTwinsEntity.getTwinUserId().trim());
              } else {
                studentTwinEntity.setCreateUser("PEN_DEMOG_MIGRATION_API");
                studentTwinEntity.setUpdateUser("PEN_DEMOG_MIGRATION_API");
              }
              studentTwinEntity.setStudentTwinReasonCode("PENMATCH");
              studentTwinEntity.setStudentID(student1.getStudentID());
              studentTwinEntity.setTwinStudent(student2);
              twinEntities.add(studentTwinEntity);
            } else {
              log.debug("Record is present. for PEN :: {} and PEN :: {}", penTwinsEntity.getPenTwin1().trim(), penTwinsEntity.getPenTwin2().trim());
            }
          } else {
            log.info("Ignoring this record as there is no student record  for PEN :: {}", penTwinsEntity.getPenTwin2().trim());
          }
        } else {
          log.info("Ignoring this record as there is no student record  for PEN :: {}", penTwinsEntity.getPenTwin1().trim());
        }

      });
      if (!twinEntities.isEmpty()) {
        log.info("created {} twinned entities", twinEntities.size());
        getStudentTwinService().saveTwinnedEntities(twinEntities);
      }
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
