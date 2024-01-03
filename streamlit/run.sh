#!/usr/bin/env bash

if [ ! -f config.ini.template ]
then
  ln -s ../config.ini.template
fi

if [ ! -f jdbc-stress-tool-1.0-jar-with-dependencies.jar ]
then
  ln -s ../target/jdbc-stress-tool-1.0-jar-with-dependencies.jar
fi

if [ ! -f clickzetta-java-1.2.2.jar ]
then
  wget https://repo1.maven.org/maven2/com/clickzetta/clickzetta-java/1.2.2/clickzetta-java-1.2.2.jar
fi

streamlit run --browser.gatherUsageStats false --enableCORS false --enableXsrfProtection false main.py
