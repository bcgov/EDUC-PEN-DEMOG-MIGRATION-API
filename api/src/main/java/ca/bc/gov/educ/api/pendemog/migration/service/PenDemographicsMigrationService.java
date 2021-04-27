package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.CounterUtil;
import ca.bc.gov.educ.api.pendemog.migration.constants.HistoryActivityCode;
import ca.bc.gov.educ.api.pendemog.migration.constants.MatchReasonCode;
import ca.bc.gov.educ.api.pendemog.migration.constants.StudentMergeSourceCodes;
import ca.bc.gov.educ.api.pendemog.migration.exception.CodeNotFoundException;
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
import java.time.format.DateTimeParseException;
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
      this.studNoSet.add("" + i);
    }
    this.studNoSet.add("2");
    this.studNoSet.add("3");
    this.studNoSet.add("4");
    this.studNoSet.add("5");
    this.studNoSet.add("6");
    this.studNoSet.add("7");
    for (var i = 8000; i < 10000; i++) {
      this.studNoSet.add("" + i);
    }

    log.info("init method completed.");
  }

  @Autowired
  public PenDemographicsMigrationService(final EntityManager entityManager, final ApplicationProperties applicationProperties, final PenDemographicsMigrationRepository penDemographicsMigrationRepository, final PenAuditRepository penAuditRepository, final StudentRepository studentRepository, final StudentHistoryRepository studentHistoryRepository, final StudentMergeRepository studentMergeRepository, final PenMergeRepository penMergeRepository, final PenTwinRepository penTwinRepository, final PossibleMatchRepository possibleMatchRepository, final StudentTwinService studentTwinService, final StudentService studentService) {
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
    this.executorService = Executors.newFixedThreadPool(applicationProperties.getQueryThreads());
  }


  /**
   * Process data migration.
   */
  public void processDataMigration() {
    this.processDemogDataMigration();
    this.processDemogAuditDataMigration();
    this.processMigrationOfTwins();
    this.processMigrationOfMerges();
  }

  public void processDemogAuditDataMigration() {
    final Query countQuery = this.entityManager.createNativeQuery("SELECT COUNT(1) FROM API_STUDENT.STUDENT");
    final List countQueryResultList = countQuery.getResultList();
    final int chunkSize = 10000;
    BigDecimal totalStudentRecords = null;
    if (countQueryResultList != null && !countQueryResultList.isEmpty()) {
      totalStudentRecords = (BigDecimal) countQueryResultList.get(0);
    }
    assert totalStudentRecords != null;
    final int totalIteration = totalStudentRecords.intValue() / chunkSize;
    log.info("Total number of iteration is  :: {} ", totalIteration);
    final List<RowFilter> chunkList = new ArrayList<>();
    int high;
    int low = 0;
    for (int i = 0; i <= totalIteration; i++) {
      high = low + chunkSize;
      chunkList.add(RowFilter.builder().low(low).high(high).build());
      low = high;
    }
    final List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (final var chunk : chunkList) {
      final Callable<Boolean> callable = () -> this.processDemogAuditChunk(chunk);
      futures.add(this.auditExecutor.submit(callable));
    }
    this.checkFutureResults(futures);
    log.info("All pen demog audit records have been processed.");
  }

  private void checkFutureResults(final List<Future<Boolean>> futures) {
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      for (final var future : futures) {
        try {
          future.get();
        } catch (final InterruptedException | ExecutionException e) {
          Thread.currentThread().interrupt();
          log.warn("Error waiting for result", e);
        }
      }
    }
  }

  private Boolean processDemogAuditChunk(final RowFilter chunk) {
    final Query penStudIdQuery = this.entityManager.createNativeQuery("select *\n" +
        "from (select row_.*, rownum rownum_\n" +
        "      from (select PEN, STUDENT_ID\n" +
        "            from API_STUDENT.STUDENT\n" +
        "            order by PEN) row_\n" +
        "      where rownum <=" + chunk.getHigh() + " )\n" +
        "where rownum_ > " + chunk.getLow());
    final List<Object[]> penStudIdList = penStudIdQuery.getResultList();
    final List<StudentHistoryEntity> historyEntitiesToPersist = new CopyOnWriteArrayList<>();
    final List<StudentEntity> studentEntities = new CopyOnWriteArrayList<>();
    for (final var penStud : penStudIdList) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) penStud[1]);
      final Long highBits = byteBuffer.getLong();
      final Long lowBits = byteBuffer.getLong();

      final var guid = new UUID(highBits, lowBits);
      log.trace("pen number and student id is :: {}, {}", penStud[0], guid);
      final StudentEntity entity = new StudentEntity();
      entity.setPen(String.valueOf(penStud[0]));
      entity.setStudentID(guid);
      studentEntities.add(entity);
    }

    final var results = Lists.partition(studentEntities, this.partitionSize);
    for (final var result : results) {
      historyEntitiesToPersist.addAll(this.processStudentEntityList(result));
    }
    if (!historyEntitiesToPersist.isEmpty()) {
      try {
        log.info("going to persist {} history records", historyEntitiesToPersist.size());
        this.studentService.saveHistoryEntities(historyEntitiesToPersist);
      } catch (final Exception ex) {
        log.error("exception while saving history entities", ex);
      }
    }
    log.info("processing complete for chunk , low :: {}, high :: {}, processed {} many pens", chunk.getLow(), chunk.getHigh(), penStudIdList.size());
    return true;
  }

  private List<StudentHistoryEntity> processStudentEntityList(final List<StudentEntity> studentEntities) {
    final List<String> penList = new CopyOnWriteArrayList<>();
    final List<UUID> studentIdList = new CopyOnWriteArrayList<>();
    final Map<String, UUID> penStudIDMap = new ConcurrentHashMap<>();
    for (final var item : studentEntities) {
      penStudIDMap.put(item.getPen(), item.getStudentID());
      penList.add(item.getPen() + " ");
      studentIdList.add(item.getStudentID());
    }
    final var penAuditEntities = new CopyOnWriteArrayList<>(this.getPenAuditRepository().findByPenIn(penList));
    if (!penAuditEntities.isEmpty()) {
      final List<StudentHistoryEntity> studentHistoryEntities = new CopyOnWriteArrayList<>(this.getStudentHistoryRepository().findAllByStudentIDIn(studentIdList));
      log.debug("Pen Audit entities before filter :: {}", penAuditEntities.size());
      if (!studentHistoryEntities.isEmpty()) {
        for (final var penAuditEntity : penAuditEntities) {
          for (final var studentHistory : studentHistoryEntities) {
            if (penAuditEntity != null && studentHistory != null
                && StringUtils.equals(StringUtils.trim(penAuditEntity.getPen()), studentHistory.getPen())
                && StringUtils.isNotBlank(penAuditEntity.getActivityDate()) && studentHistory.getCreateDate().isEqual(this.getLocalDateTimeFromString(penAuditEntity.getActivityDate()))
                && StringUtils.equals(StringUtils.trim(penAuditEntity.getCreateUser()), studentHistory.getCreateUser())
                && StringUtils.equals(this.getHistoryActivityCode(StringUtils.trim(penAuditEntity.getAuditCode())), studentHistory.getHistoryActivityCode())) {
              studentHistoryEntities.remove(studentHistory);
              penAuditEntities.remove(penAuditEntity);
            }
          }
        }
      }
      log.debug("Pen Audit entities after filter :: {}", penAuditEntities.size());
      if (!penAuditEntities.isEmpty()) {
        return this.studentService.processDemographicsAuditEntities(penAuditEntities, penStudIDMap);
      }
    }
    return Collections.emptyList();
  }


  private LocalDateTime getLocalDateTimeFromString(String dateTime) {
    try {
      if(StringUtils.isNotBlank(dateTime)) {
        dateTime = dateTime.trim();
        dateTime = StringUtils.substring(dateTime, 0, 19);
        final var pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(dateTime, pattern);
      }
      return LocalDateTime.now();
    }catch (Exception e){
      return LocalDateTime.now();
    }
  }

  private LocalDate getLocalDateFromString(String date) {
    try {
      var pattern = DateTimeFormatter.ofPattern("yyyyMMdd");
      return LocalDate.parse(date, pattern);
    }catch(Exception e){
      return LocalDate.now();
    }
  }


  public void processDemogDataMigration() {
    final List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (final String studNo : this.studNoSet) {
      final Callable<Boolean> callable = () -> this.processDemog(studNo);
      futures.add(this.executorService.submit(callable));
    }
    this.checkFutureResults(futures);
    log.info("All pen demog records have been processed, moving to next phase");
  }

  private Boolean processDemog(final String studNoLike) {
    log.debug("Now Processing studNo starting with :: {}", studNoLike);
    final List<PenDemographicsEntity> penDemographicsEntities = new CopyOnWriteArrayList<>(this.getPenDemographicsMigrationRepository().findByStudNoLike(studNoLike + "%"));
    if (!penDemographicsEntities.isEmpty()) {
      log.debug("Found {} records from pen demog for Stud No :: {}", penDemographicsEntities.size(), studNoLike);
      final List<StudentEntity> studentEntities = new CopyOnWriteArrayList<>(this.getStudentRepository().findByPenLike(studNoLike + "%"));
      log.debug("Found {} records from student for pen :: {}", studentEntities.size(), studNoLike);
      if (!studentEntities.isEmpty()) {
        for (final var penDemogEntity : penDemographicsEntities) {
          for (final var student : studentEntities) {
            if (StringUtils.equals(StringUtils.trim(penDemogEntity.getStudNo()), StringUtils.trim(student.getPen()))) {
              penDemographicsEntities.remove(penDemogEntity);
              studentEntities.remove(student);
            }
          }
        }
      }
      if (!penDemographicsEntities.isEmpty()) {
        log.debug("Found {} records for studNo starting with {} which are not processed and now processing.", penDemographicsEntities.size(), studNoLike);
        return this.studentService.processDemographicsEntities(penDemographicsEntities, studNoLike);
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
    final List<StudentMergeEntity> mergeFromEntities = new ArrayList<>();
    final List<StudentMergeEntity> mergeTOEntities = new ArrayList<>();
    final List<StudentEntity> mergedStudents = new ArrayList<>();
    final var penMerges = this.penMergeRepository.findAll();
    if (!penMerges.isEmpty()) {
      this.createMergedRecords(penMerges, mergeFromEntities, mergeTOEntities, mergedStudents);
      final List<List<StudentMergeEntity>> mergeFromSubset = Lists.partition(mergeFromEntities, 1000);
      final List<List<StudentMergeEntity>> mergeToSubset = Lists.partition(mergeTOEntities, 1000);
      final List<List<StudentEntity>> mergedStudentsSubset = Lists.partition(mergedStudents, 1000);
      log.info("created subset of {} merge from  entities", mergeFromSubset.size());
      log.info("created subset of {} merge to  entities", mergeToSubset.size());
      mergeFromSubset.forEach(this.getStudentMergeRepository()::saveAll);
      mergeToSubset.forEach(this.getStudentMergeRepository()::saveAll);
      mergedStudentsSubset.forEach(this.getStudentRepository()::saveAll);
    }
    log.info("finished data migration of Merges, persisted {} merge from  records and {} merge to records to DB", mergeFromEntities.size(), mergeTOEntities.size());
  }

  private void createMergedRecords(final List<PenMergesEntity> penMerges, final List<StudentMergeEntity> mergeFromEntities, final List<StudentMergeEntity> mergeTOEntities, final List<StudentEntity> mergedStudents) {
    final AtomicInteger counter = new AtomicInteger();
    final Map<String, List<String>> mergeEntitiesMap = new HashMap<>();
    for (final var penMerge : penMerges) {
      if (mergeEntitiesMap.containsKey(penMerge.getStudTrueNo().trim())) {
        final List<String> penNumbers = mergeEntitiesMap.get(penMerge.getStudTrueNo().trim());
        penNumbers.add(penMerge.getStudNo().trim());
        mergeEntitiesMap.put(penMerge.getStudTrueNo().trim(), penNumbers);
      } else {
        final List<String> penNumbers = new ArrayList<>();
        penNumbers.add(penMerge.getStudNo().trim());
        mergeEntitiesMap.put(penMerge.getStudTrueNo().trim(), penNumbers);
      }
    }
    log.info("Total Entries in Merges MAP {}", mergeEntitiesMap.size());
    mergeEntitiesMap.forEach(this.findAndCreateMergeEntities(mergeFromEntities, mergeTOEntities,mergedStudents, counter));
  }

  private BiConsumer<String, List<String>> findAndCreateMergeEntities(final List<StudentMergeEntity> mergeFromEntities, final List<StudentMergeEntity> mergeTOEntities, final List<StudentEntity> mergedStudents, final AtomicInteger counter) {
    return (truePen, penList) -> {
      final var originalStudent = this.studentRepository.findStudentEntityByPen(truePen);
      penList.parallelStream().forEach(this.createMergeStudentEntities(mergeFromEntities, mergeTOEntities, counter, truePen, originalStudent, mergedStudents));
    };
  }

  private Consumer<String> createMergeStudentEntities(final List<StudentMergeEntity> mergeFromEntities, final List<StudentMergeEntity> mergeTOEntities, final AtomicInteger counter, final String truePen, final Optional<StudentEntity> originalStudent, final List<StudentEntity> mergedStudents) {
    return penNumber -> {
      log.info("Index {}, creating merge from and merge to entity for true pen and pen :: {} {}", counter.incrementAndGet(), truePen, penNumber);
      final var mergedStudent = this.studentRepository.findStudentEntityByPen(penNumber);
      if (originalStudent.isPresent() && mergedStudent.isPresent()) {
        Optional<PenDemographicsEntity> penDemogs = this.getPenDemographicsMigrationRepository().findByStudNo(penNumber + " ");
        if(penDemogs.isPresent()) {
          StudentEntity mergedStudentEntity = mergedStudent.get();
          mergedStudentEntity.setTrueStudentID(originalStudent.get().getStudentID());
          mergedStudents.add(mergedStudentEntity);
          final StudentMergeEntity mergeFromEntity = this.createMergeEntity(mergedStudentEntity, originalStudent.get().getStudentID(), "FROM", penDemogs.get());
          log.debug("Index {}, merge from  entity {}", counter.get(), mergeFromEntity.toString());
          mergeFromEntities.add(mergeFromEntity);

          final StudentMergeEntity mergeTOEntity = this.createMergeEntity(originalStudent.get(), mergedStudent.get().getStudentID(), "TO", penDemogs.get());
          log.debug("Index {}, merge to  entity {}", counter.get(), mergeTOEntity.toString());
          mergeTOEntities.add(mergeTOEntity);
        } else {
          log.error("Index {}, pen demogs not for true pen and pen :: {} :: {}", counter.get(), truePen, penNumber);
        }
      } else {
        log.error("Index {}, student entity not found for true pen and pen :: {} :: {}", counter.get(), truePen, penNumber);
      }
    };
  }

  private StudentMergeEntity createMergeEntity(final StudentEntity mergeStudent, final UUID studentId, final String direction, final PenDemographicsEntity demogEntity) {
    final StudentMergeEntity mergeTOEntity = new StudentMergeEntity();

    try {
      mergeTOEntity.setStudentMergeSourceCode(findByOldMergeCode(demogEntity.getMergeToCode()).getPrrCode());
    } catch (final CodeNotFoundException e) {
      log.info("Merge source code not found for value :: {}", demogEntity.getMergeToCode());
      mergeTOEntity.setStudentMergeSourceCode(StudentMergeSourceCodes.MI.getPrrCode());
    }

    mergeTOEntity.setStudentMergeDirectionCode(direction);
    mergeTOEntity.setStudentID(studentId);
    mergeTOEntity.setMergeStudentID(mergeStudent.getStudentID());
    LocalDateTime mergeDate;
    try {
      mergeDate = this.getLocalDateTimeFromString(demogEntity.getMergeToDate());
    } catch (final DateTimeParseException e) {
      mergeDate = LocalDateTime.now();
    }
    mergeTOEntity.setCreateDate(mergeDate);
    mergeTOEntity.setUpdateDate(mergeDate);

    if(StringUtils.isNotBlank(demogEntity.getMergeToUserName())){
      mergeTOEntity.setCreateUser(demogEntity.getMergeToUserName());
      mergeTOEntity.setUpdateUser(demogEntity.getMergeToUserName());
    }else{
      mergeTOEntity.setCreateUser("PEN_MIGRATION_API");
      mergeTOEntity.setUpdateUser("PEN_MIGRATION_API");
    }

    return mergeTOEntity;
  }

  public void processMigrationOfTwins() {
    log.info("Starting data migration of Twins");
    final List<Future<Boolean>> futures = new CopyOnWriteArrayList<>();
    for (final String studNo : this.studNoSet) {
      final Callable<Boolean> callable = () -> this.processTwinForPenLike(studNo);
      futures.add(this.executorService.submit(callable));
    }
    if (!futures.isEmpty()) {
      log.info("waiting for future results. futures size is :: {}", futures.size());
      int index = 1;
      for (final var future : futures) {
        try {
          future.get();
          log.info("Total completed is :: {}", index++);
        } catch (final InterruptedException | ExecutionException e) {
          log.warn("Error waiting for result", e);
        }
      }
    }
    log.info("All pen twin records have been processed, moving to next phase");
    //TWIN_REASON_CODE=PENMATCH

  }

  private Boolean processTwinForPenLike(final String penLike) {
    final List<PossibleMatchEntity> twinEntities = new CopyOnWriteArrayList<>();
    final var penTwins = this.getPenTwinRepository().findByPenTwin1Like(penLike + "%");
    final var studentTwins = this.getStudentRepository().findByPenLike(penLike + "%");
    final var studentTwinMap = studentTwins.stream()
            .collect(Collectors.toMap(StudentEntity::getPen, studentEntity -> studentEntity));
    log.info("found {} records .", penTwins.size());
    if (!penTwins.isEmpty()) {
      penTwins.forEach(penTwinsEntity -> {
        final StudentEntity student1;
        final StudentEntity student2;
        var studentEntity1 = Optional.ofNullable(studentTwinMap.get(penTwinsEntity.getPenTwin1().trim()));
        if (studentEntity1.isEmpty()) {
          studentEntity1 = this.getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin1().trim());
        }
        if (studentEntity1.isPresent()) {
          var studentEntity2 = Optional.ofNullable(studentTwinMap.get(penTwinsEntity.getPenTwin2().trim()));
          if (studentEntity2.isEmpty()) {
            studentEntity2 = this.getStudentRepository().findStudentEntityByPen(penTwinsEntity.getPenTwin2().trim());
          }
          if (studentEntity2.isPresent()) {
            student1 = studentEntity1.get();
            student2 = studentEntity2.get();
            final Optional<PossibleMatchEntity> dbEntity = this.getPossibleMatchRepository().findByStudentIDAndMatchedStudentID(student1.getStudentID(), student2.getStudentID());
            if (dbEntity.isEmpty()) {
              final PossibleMatchEntity possibleMatchEntity = new PossibleMatchEntity();
              LocalDate twinDate;
              try {
                twinDate = this.getLocalDateFromString(penTwinsEntity.getTwinDate());
              } catch (final DateTimeParseException e) {
                twinDate = LocalDate.now();
              }
              possibleMatchEntity.setCreateDate(twinDate.atStartOfDay());
              possibleMatchEntity.setUpdateDate(twinDate.atStartOfDay());
              if (penTwinsEntity.getTwinUserId() != null && !"".equalsIgnoreCase(penTwinsEntity.getTwinUserId().trim())) {
                possibleMatchEntity.setCreateUser(penTwinsEntity.getTwinUserId().trim());
                possibleMatchEntity.setUpdateUser(penTwinsEntity.getTwinUserId().trim());
              } else {
                possibleMatchEntity.setCreateUser("PEN_MIGRATION_API");
                possibleMatchEntity.setUpdateUser("PEN_MIGRATION_API");
              }
              try {
                possibleMatchEntity.setMatchReasonCode(findByOldCode(penTwinsEntity.getTwinReason()).getPrrCode());
              } catch (final CodeNotFoundException e) {
                log.info("Match reason code not found for value :: {}", penTwinsEntity.getTwinReason());
                possibleMatchEntity.setMatchReasonCode(MatchReasonCode.PENMATCH.getPrrCode());
              }

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
        this.getStudentTwinService().saveTwinnedEntities(twinEntities);
      }
    }
    return true;
  }

  private StudentMergeSourceCodes findByOldMergeCode(final String oldCode) throws CodeNotFoundException {
    return Arrays.stream(StudentMergeSourceCodes.values()).filter(value -> value.getOldCode().equals(oldCode)).findFirst().orElseThrow(() -> new CodeNotFoundException());
  }

  private MatchReasonCode findByOldCode(final String oldCode) throws CodeNotFoundException {
    return Arrays.stream(MatchReasonCode.values()).filter(value -> value.getOldCode().equals(oldCode)).findFirst().orElseThrow(() -> new CodeNotFoundException());
  }

  @Override
  public void close() {
    if (!this.executorService.isShutdown()) {
      this.executorService.shutdown();
    }

  }

  private String getHistoryActivityCode(final String auditCode) {
    if (auditCode != null) {
      return auditCode.trim().equalsIgnoreCase("A") ? HistoryActivityCode.USER_NEW.getCode() : HistoryActivityCode.USER_EDIT.getCode();
    }
    return HistoryActivityCode.USER_NEW.getCode();
  }
}
