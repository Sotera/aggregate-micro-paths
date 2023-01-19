#!/bin/bash

# fix_slf4j.sh
# Jacobs 2023, @ctwardy
# 
# Fix the harmless but annoying SLF4J multiple-bindings warning.
# Rename the binding that Spark/Hive weren't using anyway.

# In our case, this was the extraneous binding:
JARDIR=/opt/cloudera/parcels/CDH/jars/
BASENAME=log4j-slf4j-impl-2.8.2
OLD=${BASENAME}.jar 
NEW=${BASENAME}.XXX

if [ -f ${JARDIR}/${OLD} ]; then
  echo "Moving ${OLD} to ${NEW} to suppress SLF4J warnings." 
  sudo mv ${JARDIR}/${OLD} ${JARDIR}/${NEW}
else
  if [ ! -f ${JARDIR}/${NEW} ]; then
    echo "${BASENAME}.* not found in ${JARDIR}."
    echo "If getting SLF4J warnings, modify this script for named jarfile(s)."
  fi
  echo "Nothing to be done."
fi
