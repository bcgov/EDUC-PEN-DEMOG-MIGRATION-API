package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.HistoryActivityCode;
import ca.bc.gov.educ.api.pendemog.migration.model.*;
import ca.bc.gov.educ.api.pendemog.migration.properties.ApplicationProperties;
import ca.bc.gov.educ.api.pendemog.migration.repository.*;
import ca.bc.gov.educ.api.pendemog.migration.struct.RowFilter;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.Closeable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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


  private final Integer partitionSize;
  private final EntityManager entityManager;
  private final ExecutorService executorService;
  private final ExecutorService auditExecutor = Executors.newFixedThreadPool(40);
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
  private final PossibleMatchRepository possibleMatchRepository;
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
  public PenDemographicsMigrationService(EntityManager entityManager, ApplicationProperties applicationProperties, final PenDemographicsMigrationRepository penDemographicsMigrationRepository, PenAuditRepository penAuditRepository, StudentRepository studentRepository, StudentHistoryRepository studentHistoryRepository, StudentMergeRepository studentMergeRepository, PenMergeRepository penMergeRepository, PenTwinRepository penTwinRepository, PossibleMatchRepository possibleMatchRepository, StudentTwinService studentTwinService, StudentService studentService) {
    this.partitionSize = applicationProperties.getPartitionSize();
    this.entityManager = entityManager;
    this.penDemographicsMigrationRepository = penDemographicsMigrationRepository;
    this.penAuditRepository = penAuditRepository;
    this.studentRepository = studentRepository;
    this.studentHistoryRepository = studentHistoryRepository;
    this.studentMergeRepository = studentMergeRepository;
    this.penMergeRepository = penMergeRepository;
    this.penTwinRepository = penTwinRepository;
    this.possibleMatchRepository = possibleMatchRepository;
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
    Query countQuery = entityManager.createNativeQuery("SELECT COUNT(1) FROM API_STUDENT.STUDENT");
    List countQueryResultList = countQuery.getResultList();
    int chunkSize = 10000;
    BigDecimal totalStudentRecords = null;
    if (countQueryResultList != null && !countQueryResultList.isEmpty()) {
      totalStudentRecords = (BigDecimal) countQueryResultList.get(0);
    }
    assert totalStudentRecords != null;
    int totalIteration = totalStudentRecords.intValue() / chunkSize;
    log.info("Total number of iteration is  :: {} ", totalIteration);
    List<RowFilter> chunkList = new ArrayList<>();
    int high;
    int low = 0;
    for (int i = 0; i <= totalIteration; i++) {
      high = low + chunkSize;
      chunkList.add(RowFilter.builder().low(low).high(high).build());
      low = high;
    }
    List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (var chunk : chunkList) {
      final Callable<Boolean> callable = () -> processDemogAuditChunk(chunk);
      futures.add(auditExecutor.submit(callable));
    }
    checkFutureResults(futures);
    log.info("All pen demog audit records have been processed.");
  }

  private void checkFutureResults(List<Future<Boolean>> futures) {
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
  }

  private Boolean processDemogAuditChunk(RowFilter chunk) {
    Query penStudIdQuery = entityManager.createNativeQuery("select *\n" +
        "from (select row_.*, rownum rownum_\n" +
        "      from (select PEN, STUDENT_ID\n" +
        "            from API_STUDENT.STUDENT\n" +
        "            order by PEN) row_\n" +
        "      where rownum <=" + chunk.getHigh() + " )\n" +
        "where rownum_ > " + chunk.getLow());
    List<Object[]> penStudIdList = penStudIdQuery.getResultList();
    List<StudentHistoryEntity> historyEntitiesToPersist = new CopyOnWriteArrayList<>();
    List<StudentEntity> studentEntities = new CopyOnWriteArrayList<>();
    for (var penStud : penStudIdList) {
      ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) penStud[1]);
      Long highBits = byteBuffer.getLong();
      Long lowBits = byteBuffer.getLong();

      var guid = new UUID(highBits, lowBits);
      log.debug("pen number and student id is :: {}, {}", penStud[0], guid);
      StudentEntity entity = new StudentEntity();
      entity.setPen(String.valueOf(penStud[0]));
      entity.setStudentID(guid);
      studentEntities.add(entity);
    }

    var results = Lists.partition(studentEntities, this.partitionSize);
    for (var result : results) {
      historyEntitiesToPersist.addAll(processStudentEntityList(result));
    }
    if (!historyEntitiesToPersist.isEmpty()) {
      try {
        studentService.saveHistoryEntities(historyEntitiesToPersist);
      } catch (final Exception ex) {
        log.error("exception while saving history entities", ex);
      }
    }
    log.info("processing complete for chunk , low :: {}, high :: {}, processed {} many pens", chunk.getLow(), chunk.getHigh(), penStudIdList.size());
    return true;
  }

  private List<StudentHistoryEntity> processStudentEntityList(List<StudentEntity> studentEntities) {
    List<String> penList = new CopyOnWriteArrayList<>();
    List<UUID> studentIdList = new CopyOnWriteArrayList<>();
    Map<String, UUID> penStudIDMap = new ConcurrentHashMap<>();
    for (var item : studentEntities) {
      penStudIDMap.put(item.getPen(), item.getStudentID());
      penList.add(item.getPen());
      studentIdList.add(item.getStudentID());
    }
    var penAuditEntities = new CopyOnWriteArrayList<>(getPenAuditRepository().findByPenIn(penList));
    if (!penAuditEntities.isEmpty()) {
      List<StudentHistoryEntity> studentHistoryEntities = new CopyOnWriteArrayList<>(getStudentHistoryRepository().findAllByStudentIDIn(studentIdList));
      log.debug("Pen Audit entities before filter :: {}", penAuditEntities.size());
      if (!studentHistoryEntities.isEmpty()) {
        for (var penAuditEntity : penAuditEntities) {
          for (var studentHistory : studentHistoryEntities) {
            if (penAuditEntity != null && studentHistory != null
                && StringUtils.equals(StringUtils.trim(penAuditEntity.getPen()), studentHistory.getPen())
                && StringUtils.isNotBlank(penAuditEntity.getActivityDate()) && studentHistory.getCreateDate().isEqual(getLocalDateTimeFromString(penAuditEntity.getActivityDate()))
                && StringUtils.equals(StringUtils.trim(penAuditEntity.getCreateUser()), studentHistory.getCreateUser())
                && StringUtils.equals(getHistoryActivityCode(StringUtils.trim(penAuditEntity.getAuditCode())), studentHistory.getHistoryActivityCode())) {
              studentHistoryEntities.remove(studentHistory);
              penAuditEntities.remove(penAuditEntity);
            }
          }
        }
      }
      log.debug("Pen Audit entities after filter :: {}", penAuditEntities.size());
      if (!penAuditEntities.isEmpty()) {
        log.debug("Found {} records which are not processed and now processing.", penAuditEntities.size());
        return studentService.processDemographicsAuditEntities(penAuditEntities, penStudIDMap);
      }
    }
    return Collections.emptyList();
  }


  private LocalDateTime getLocalDateTimeFromString(String dateTime) {
    if (dateTime == null) {
      return null;
    } else {
      dateTime = dateTime.trim();
      dateTime = StringUtils.substring(dateTime, 0, 19);
    }
    var pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(dateTime, pattern);
  }

  private LocalDate getLocalDateFromString(String date) {
    var pattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    return LocalDate.parse(date, pattern);
  }


  public void processDemogDataMigration() {
    List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (String studNo : studNoSet) {
      final Callable<Boolean> callable = () -> processDemog(studNo);
      futures.add(executorService.submit(callable));
    }
    checkFutureResults(futures);
    log.info("All pen demog records have been processed, moving to next phase");
  }

  private Boolean processDemog(String studNoLike) {
    log.debug("Now Processing studNo starting with :: {}", studNoLike);
    List<PenDemographicsEntity> penDemographicsEntities = new CopyOnWriteArrayList<>(getPenDemographicsMigrationRepository().findByStudNoLike(studNoLike + "%"));
    if (!penDemographicsEntities.isEmpty()) {
      log.debug("Found {} records from pen demog for Stud No :: {}", penDemographicsEntities.size(), studNoLike);
      List<StudentEntity> studentEntities = new CopyOnWriteArrayList<>(getStudentRepository().findByPenLike(studNoLike + "%"));
      log.debug("Found {} records from student for pen :: {}", studentEntities.size(), studNoLike);
      if (!studentEntities.isEmpty()) {
        for (var penDemogEntity : penDemographicsEntities) {
          for (var student : studentEntities) {
            if (StringUtils.equals(StringUtils.trim(penDemogEntity.getStudNo()), StringUtils.trim(student.getPen()))) {
              penDemographicsEntities.remove(penDemogEntity);
              studentEntities.remove(student);
            }
          }
        }
      }
      if (!penDemographicsEntities.isEmpty()) {
        log.debug("Found {} records for studNo starting with {} which are not processed and now processing.", penDemographicsEntities.size(), studNoLike);
        return studentService.processDemographicsEntities(penDemographicsEntities, studNoLike);
      } else {
        log.info("Nothing to process for :: {} marking complete. total number of records processed :: {}", studNoLike, CounterUtil.processCounter.incrementAndGet());
      }
    } else {
      log.info("No Records found for Stud No like :: {} in PEN_DEMOG so skipped., total number of records processed :: {}", studNoLike, CounterUtil.processCounter.incrementAndGet());
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
    List<PossibleMatchEntity> twinEntities = new CopyOnWriteArrayList<>();
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
            Optional<PossibleMatchEntity> dbEntity = getPossibleMatchRepository().findByStudentIDAndMatchedStudentID(student1.getStudentID(), student2.getStudentID());
            if (dbEntity.isEmpty()) {
              PossibleMatchEntity possibleMatchEntity = new PossibleMatchEntity();
              possibleMatchEntity.setCreateDate(LocalDateTime.now());
              possibleMatchEntity.setUpdateDate(LocalDateTime.now());
              if (penTwinsEntity.getTwinUserId() != null && !"".equalsIgnoreCase(penTwinsEntity.getTwinUserId().trim())) {
                possibleMatchEntity.setCreateUser(penTwinsEntity.getTwinUserId().trim());
                possibleMatchEntity.setUpdateUser(penTwinsEntity.getTwinUserId().trim());
              } else {
                possibleMatchEntity.setCreateUser("PEN_DEMOG_MIGRATION_API");
                possibleMatchEntity.setUpdateUser("PEN_DEMOG_MIGRATION_API");
              }
              possibleMatchEntity.setMatchReasonCode("PENMATCH");
              possibleMatchEntity.setStudentID(student1.getStudentID());
              possibleMatchEntity.setMatchedStudentID(student2.getStudentID());
              twinEntities.add(possibleMatchEntity);
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

  private String getHistoryActivityCode(String auditCode) {
    if (auditCode != null) {
      return auditCode.trim().equalsIgnoreCase("A") ? HistoryActivityCode.USER_NEW.getCode() : HistoryActivityCode.USER_EDIT.getCode();
    }
    return HistoryActivityCode.USER_NEW.getCode();
  }
}
