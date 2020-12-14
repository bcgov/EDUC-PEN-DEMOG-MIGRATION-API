package ca.bc.gov.educ.api.pendemog.migration.service;

import ca.bc.gov.educ.api.pendemog.migration.model.PenDemographicsEntity;
import ca.bc.gov.educ.api.pendemog.migration.repository.PenDemographicsMigrationRepository;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.List;

@Slf4j
@Service
public class PENDemogPersistenceService {
  private final PenDemographicsMigrationRepository penDemogRepository;

  @Autowired
  private EntityManagerFactory emf;

  @Autowired
  public PENDemogPersistenceService(PenDemographicsMigrationRepository penDemogRepository) {
    this.penDemogRepository = penDemogRepository;
  }

  public void savePENDemog(PenDemographicsEntity penDemogEntity, String randLocalIDVal) {
    EntityManager em = emf.createEntityManager();
    EntityTransaction tx = em.getTransaction();
    tx.begin();

    try{
      em.createNativeQuery("UPDATE PEN_DEMOG@PENLINK.WORLD SET PEN_LOCAL_ID = '" +  randLocalIDVal + "' WHERE STUD_NO = '" + penDemogEntity.getStudNo().trim() + "'").executeUpdate();
      tx.commit();
    }catch(Exception e){
      log.error("Error occured saving entity " + e.getMessage());
      tx.rollback();
    }finally{
      tx = null;
      em.close();
    }
  }
}
