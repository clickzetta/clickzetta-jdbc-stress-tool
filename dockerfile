FROM openjdk:8u342-jre-slim-bullseye

FROM python:3.9-slim-bullseye
COPY --from=0 /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH "$JAVA_HOME/bin:$PATH"

RUN echo 'deb https://mirrors.aliyun.com/debian bullseye main' > /etc/apt/sources.list
RUN echo 'deb https://mirrors.aliyun.com/debian-security bullseye-security main' >> /etc/apt/sources.list
RUN echo 'deb https://mirrors.aliyun.com/debian bullseye-updates main' >> /etc/apt/sources.list
RUN apt update && apt install -y wget inetutils-ping

RUN mkdir -p /opt/lab_app
WORKDIR /opt/lab_app

# python dependencies
ADD streamlit/requirements.txt requirements.txt
RUN pip install -i https://mirrors.cloud.tencent.com/pypi/simple -r requirements.txt

# jdbc-stress-tool
ADD config.ini.template config.ini.template
RUN wget https://repo1.maven.org/maven2/com/clickzetta/clickzetta-jdbc/3.0.3/clickzetta-jdbc-3.0.3.jar
ADD target/jdbc-stress-tool-1.0-jar-with-dependencies.jar jdbc-stress-tool-1.0-jar-with-dependencies.jar

# streamlit webui
ADD streamlit/run.sh run.sh
ADD streamlit/main.py main.py
ADD streamlit/run.py run.py
ADD streamlit/view.py view.py
ADD streamlit/icon.png icon.png

# benchmarks
ADD streamlit/benchmark benchmark

ENV VOLUME /mnt/userdata
VOLUME /mnt/userdata

EXPOSE 8501
CMD [ "/opt/lab_app/run.sh" ]
