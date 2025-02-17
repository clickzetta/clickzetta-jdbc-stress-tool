#!/usr/bin/env bash

if [ ! -f config.ini.template ]
then
  ln -s ../config.ini.template
fi

if [ ! -f jdbc-stress-tool-1.0-jar-with-dependencies.jar ]
then
  ln -s ../target/jdbc-stress-tool-1.0-jar-with-dependencies.jar
fi

CLICKZETTA_DRIVER=clickzetta-jdbc-3.0.3.jar
if [ ! -f $CLICKZETTA_DRIVER ]
then
  wget https://repo1.maven.org/maven2/com/clickzetta/clickzetta-jdbc/3.0.3/$CLICKZETTA_DRIVER
fi

streamlit run --server.address=0.0.0.0 --browser.gatherUsageStats false --server.enableCORS false --server.enableXsrfProtection false main.py
