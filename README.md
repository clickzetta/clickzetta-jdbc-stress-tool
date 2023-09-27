# ClickZetta Lakehouse JDBC Stress Tool

This is a JDBC stress tool developed by ClickZetta team.

1. Customizable SQL files and repeat times
2. Customizable JDBC driver, not only ClickZetta Lakehouse
3. Customizable concurrency
4. A streamlit powered WebUI for analyzing and visualization

Overview
![overview](overview.png)
Screenshot
![screenshot](screenshot.png)

## Getting Started

### Run in docker

1. Pull image from [dockerhub](https://hub.docker.com/r/clickzetta/jdbc-stress-tool/tags) `docker pull clickzetta/jdbc-stress-tool:dev`
2. `docker run -p 8501:8501 -v .:/mnt/userdata clickzetta/jdbc-stress-tool:dev`, test data will be stored at local path `./`,  change it as you wish.
3. Open http://localhost:8501 in your browser.

### Local compile and deploy

#### Compile jdbc-stress-tool

Prepare Java(8+) development environment as well as maven.

`mvn package`

#### Run WebUI

Prepare Python(3.9+) environment.

Get dependency packages installed
```shell
pip install -r streamlit/requirements.txt
```

Start WebUI
```shell
cd streamlit
./run.sh
```
