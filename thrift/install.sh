#!/bin/bash
#mvn deploy:deploy-file -Durl=http://maven2.eharmony.com:8080/archiva/repository/internal -DrepositoryId=eharmony.release -DpomFile=${pom} -Dfile=${jar}
mvn install:install-file -Dfile=libthrift.jar -DgroupId=com.facebook.thrift -DartifactId=libthrift -Dversion=20080411p1 -Dpackaging=jar -DgeneratePom=true