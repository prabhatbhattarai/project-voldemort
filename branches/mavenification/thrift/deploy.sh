#!/bin/bash
SITE_DIR=${1}
if [ "${SITE_DIR}" == "" ]; then
	echo "Usage: "${0} { path to where project-voldemort site is checked out }
	echo "       i.e. ../../site/www/maven"
	exit 1
fi

if [ ! -d ${SITE_DIR} ]; then
	echo "${SITE_DIR} is not a valid directory"
	echo "Note, the path must be a valid path that can be appended to a Java file://"
	exit 1
fi

mvn deploy:deploy-file -Durl=file://${SITE_DIR} -DpomFile=pom.xml -Dfile=libthrift.jar