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

```shell
docker pull clickzetta/jdbc-stress-tool:dev
mkdir stress-test
cd stress-test
docker run -p 8501:8501 -v .:/mnt/userdata clickzetta/jdbc-stress-tool:dev
```

Open http://localhost:8501 in your browser.

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
