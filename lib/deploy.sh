#!/bin/bash
COMMAND=${1}
NAME=${2}
SITE=${3}

INVALID=0
if [ "${COMMAND}" == "" -o "${NAME}" == "" ]; then
	INVALID=1
elif [ "${COMMAND}" == "deploy" -a "${SITE}" == "" ]; then
	INVALID=1
elif [ "${COMMAND}" != "install" -a "${COMMAND}" != "deploy" ]; then
	INVALID=1
fi

if [ ${INVALID} -gt 0 ]; then
	echo "Usage:    ${0} { install | deploy } { name } [ site path* ]"
	echo "          * site_path is the directory where project-voldemort"
	echo "            maven repository is checked out, and is required"
	echo "            if deploy is specified."
	echo "Examples: ${0} deploy libthrift ../../site/www/maven"
	echo "          ${0} install libthrift"
	exit 1
fi

if [ ! -d ${SITE} ]; then
	echo "${SITE} is not a valid directory"
	echo "Note, the path must be a valid path that can be appended to a Java file://"
	exit 1
fi

if [ ! -f ${NAME}.pom ]; then
	echo "${NAME}.pom is not a valid file."
	exit 1
fi
if [ ! -f ${NAME}.jar ]; then
	echo "${NAME}.jar is not a valid file."
	exit 1
fi

if [ "${COMMAND}" == "deploy" ]; then
	mvn deploy:deploy-file -Durl=file://${SITE} -DpomFile=${NAME}.pom -Dfile=${NAME}.jar
else
	mvn install:install-file -Dfile=${NAME}.jar -DpomFile=${NAME}.pom
fi