#!/usr/bin/env bash

if [ ! -f config.ini.template ]
then
  ln -s ../config.ini.template
fi

if [ ! -f log4j.properties ]
then
  ln -s ../log4j.properties
fi

if [ ! -f jdbc-stress-tool-1.0-jar-with-dependencies.jar ]
then
  ln -s ../target/jdbc-stress-tool-1.0-jar-with-dependencies.jar
fi

if [ ! -f clickzetta-java-1.0.1-jar-with-dependencies.jar ]
then
  wget https://autolake-dev-beijing.oss-cn-beijing.aliyuncs.com/clickzetta-tool/release/clickzetta-java-1.0.1-jar-with-dependencies.jar
fi

streamlit run --browser.gatherUsageStats false main.py
