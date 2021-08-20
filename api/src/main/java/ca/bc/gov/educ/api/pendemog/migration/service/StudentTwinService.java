package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.PossibleMatchEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.PossibleMatchRepository;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
public class StudentTwinService {

  @Getter(AccessLevel.PRIVATE)
  private final PossibleMatchRepository possibleMatchRepository;

  @Autowired
  public StudentTwinService(PossibleMatchRepository possibleMatchRepository) {
    this.possibleMatchRepository = possibleMatchRepository;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Retryable(value = {Exception.class}, maxAttempts = 5, backoff = @Backoff(multiplier = 3, delay = 2000))
  public void saveTwinnedEntities(List<PossibleMatchEntity> twinEntities){
    try{
      if (twinEntities.size() > 1000) {
        List<List<PossibleMatchEntity>> subSets = Lists.partition(twinEntities, 1000);
        subSets.forEach(studentTwinEntities -> getPossibleMatchRepository().saveAll(studentTwinEntities));
      } else {
        getPossibleMatchRepository().saveAll(twinEntities);
      }
    }catch (final  Exception e){
      log.error("Exception", e);
      throw e;
    }
  }
}
