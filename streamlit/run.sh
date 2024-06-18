#!/usr/bin/env bash

if [ ! -f config.ini.template ]
then
  ln -s ../config.ini.template
fi

if [ ! -f jdbc-stress-tool-1.0-jar-with-dependencies.jar ]
then
  ln -s ../target/jdbc-stress-tool-1.0-jar-with-dependencies.jar
fi

if [ ! -f clickzetta-java-1.4.6.jar ]
then
  wget https://repo1.maven.org/maven2/com/clickzetta/clickzetta-java/1.4.6/clickzetta-java-1.4.6.jar
fi

streamlit run --browser.gatherUsageStats false --server.enableCORS false --server.enableXsrfProtection false main.py
