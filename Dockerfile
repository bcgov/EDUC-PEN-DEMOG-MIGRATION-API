FROM artifacts.developer.gov.bc.ca/docker-remote/maven:3-jdk-11 as build
WORKDIR /workspace/app

COPY api/pom.xml .
COPY api/src src
RUN mvn package -DskipTests
RUN mkdir -p target/dependency && (cd target/dependency; jar -xf ../*.jar)

FROM artifacts.developer.gov.bc.ca/docker-remote/openjdk:11-jdk
RUN useradd -ms /bin/bash spring
RUN mkdir -p /logs
RUN chown -R spring:spring /logs
RUN chmod 755 /logs
USER spring
VOLUME /tmp
ARG DEPENDENCY=/workspace/app/target/dependency
COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app
ENTRYPOINT ["java","-Duser.name=PEN_DEMOG_API","-Xms14512m","-Xmx14512m","-noverify","-XX:TieredStopAtLevel=1","-XX:+UseParallelGC","-XX:MinHeapFreeRatio=5","-XX:MaxHeapFreeRatio=40","-XX:GCTimeRatio=4","-XX:AdaptiveSizePolicyWeight=90","-XX:MaxMetaspaceSize=1000m","-XX:ParallelGCThreads=8","-Djava.util.concurrent.ForkJoinPool.common.parallelism=16","-XX:CICompilerCount=8","-XX:+ExitOnOutOfMemoryError","-Djava.security.egd=file:/dev/./urandom","-Dspring.backgroundpreinitializer.ignore=true","-cp","app:app/lib/*","ca.bc.gov.educ.api.pendemog.migration.PenDemographicsDataMigrationApiResourceApplication"]
