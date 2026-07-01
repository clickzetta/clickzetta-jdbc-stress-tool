import os
from datetime import datetime
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import signal
import time
from io import StringIO
from threading import Thread
from contextlib import contextmanager, redirect_stdout
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import altair as alt
import glob
from zipfile import ZipFile, is_zipfile

from report import render_report, percentile, RENDER_LIMIT

def find_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None

    latest_file = max(files, key=os.path.getmtime)
    return latest_file

CLICKZETTA_DRIVER = os.environ.get('CLICKZETTA_DRIVER') or \
    find_latest_file('clickzetta-jdbc-*.jar') or \
    find_latest_file('clickzetta-java-*.jar')

st.title('JDBC Stress Test Runner')
col_conf_and_run, col_load_and_log = st.columns(2)

if 'VOLUME' in os.environ: # for docker
    vol = os.environ['VOLUME']
    for path in ['conf', 'sql', 'jdbc_jar', 'data', 'download']:
        src = os.path.join(vol, path)
        if not os.path.exists(src):
            os.mkdir(src)
        if not os.path.exists(path):
            os.symlink(src, path)
else:
    for path in ['conf', 'sql', 'jdbc_jar', 'data', 'download']:
        if not os.path.exists(path):
            os.mkdir(path)

def store_value(key):
    st.session_state[key] = st.session_state["_"+key]

def load_value(key):
    if key in st.session_state:
        st.session_state["_"+key] = st.session_state[key]

def clear_value(key):
    if key in st.session_state:
        st.session_state.pop(key)
        st.session_state.pop("_"+key)

@contextmanager
def st_capture(output_func):
    with StringIO() as stdout, redirect_stdout(stdout):
        old_write = stdout.write

        def new_write(string):
            ret = old_write(string)
            output_func(stdout.getvalue())
            return ret

        stdout.write = new_write
        yield

def save_file(file, folder):
    if file:
        dest = Path(folder) / file.name
        dest.write_bytes(file.read())
        if is_zipfile(dest):
            st.toast(f'Extracting {dest} to {folder}')
            with ZipFile(dest, 'r') as zip:
                zip.extractall(folder)
            dest.unlink()

def save_files(files, folder):
    if files:
        for f in files:
            save_file(f, folder)

def list_files(folder, recursive=False):
    ret = []
    files = os.listdir(folder)
    if files:
        if recursive:
            ret.append(folder)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        for f in files:
            p = os.path.join(folder, f)
            if os.path.isfile(p):
                ret.append(p)
            elif recursive and os.path.isdir(p):
                ret.extend(list_files(p, recursive))
    return ret

def monitor_and_display_log(filename):
    try:
        with open(filename) as f, st_capture(stdout.text):
            f.seek(0,2)
            while 'running_pid' in st.session_state:
                line = f.readline()
                if not line:
                    time.sleep(1)
                    continue
                print(line, end='')
            for line in f.readlines():
                print(line, end='')
    except:
        pass

def load_and_display_log(log_file):
    try:
        with open(log_file) as f:
            log = f.read()
        stdout.text(log)
    except:
        pass

@st.dialog("Upload SQL files")
def upload_sql_dialog():
    dest = 'sql'
    cols = st.columns(2)
    cols[0].markdown(f'To folder `{dest}/`')
    sub_folder = cols[1].text_input('Sub folder', label_visibility='collapsed', placeholder='sub folder if needed')
    staged = st.file_uploader(f'To folder {dest}', accept_multiple_files=True, label_visibility='collapsed')
    if st.button('OK', use_container_width=True):
        dest_path = dest
        if sub_folder:
            dest_path = os.path.join(dest, sub_folder)
        if not os.path.exists(dest_path):
            os.mkdir(dest_path)
        save_files(staged, dest_path)
        st.rerun()

@st.dialog("Upload conf file")
def upload_conf_dialog():
    dest = 'conf'
    st.markdown(f'To folder `{dest}/`')
    staged = st.file_uploader(f'To folder {dest}', accept_multiple_files=False, label_visibility='collapsed')
    if st.button('OK', use_container_width=True):
        if not os.path.exists(dest):
            os.mkdir(dest)
        save_file(staged, dest)
        st.rerun()

@st.dialog("Upload jar file")
def upload_jar_dialog():
    dest = 'jdbc_jar'
    st.markdown(f'To folder `{dest}/`')
    st.warning('Upload Clickzetta JDBC only if built-in version is not satisfied')
    staged = st.file_uploader(f'To folder {dest}', accept_multiple_files=False, label_visibility='collapsed')
    if st.button('OK', use_container_width=True):
        if not os.path.exists(dest):
            os.mkdir(dest)
        save_file(staged, dest)
        st.rerun()

with col_conf_and_run:
    st.subheader('1. Define stress: SQLs and concurreny')
    uploaded_sqls = None
    cols = st.columns([1,3], vertical_alignment='bottom')
    with cols[0]:
        if st.button('New SQL files', use_container_width=True):
            upload_sql_dialog()
    all_sqls = list_files('sql', recursive=True)
    tpc_h = list_files(os.path.join('benchmark', 'tpc-h'), recursive=True)
    ssb_flat = list_files(os.path.join('benchmark', 'ssb-flat'), recursive=True)
    load_value('selected_sqls')
    existing_sqls = cols[1].multiselect(f'Select SQL files or folders',
                                        all_sqls + ssb_flat + tpc_h,
                                        key='_selected_sqls', on_change=store_value, args=['selected_sqls'],
                                        placeholder='Pick SQL files here',
                                        help='Select a folder means all files and folders recursively in it')
    cols = st.columns(2)
    load_value('sql_repeat_time')
    repeat = cols[0].number_input('Repeat times of SQLs', value=100, min_value=1, step=1,
                                  key='_sql_repeat_time', on_change=store_value, args=['sql_repeat_time'])
    load_value('jdbc_thread')
    thread = cols[1].number_input('JDBC Concurrency', value=20, min_value=1, step=1,
                                  key='_jdbc_thread', on_change=store_value, args=['jdbc_thread'])
    st.subheader('2. Define target: config and driver')
    cols = st.columns([1,3])
    with cols[0]:
        if st.button('New conf files', use_container_width=True):
            upload_conf_dialog()
    all_confs = list_files('conf')
    load_value('selected_conf')
    existing_conf = cols[1].selectbox('Select config file', all_confs, index=None,
                                      key='_selected_conf', on_change=store_value, args=['selected_conf'],
                                      placeholder='Pick config file here', label_visibility='collapsed')
    if existing_conf:
        with st.expander('Preview selected config'):
            with open(existing_conf) as f:
                conf_content = f.read()
            st.code(conf_content, language='ini')
    with st.expander('Config file template'):
        with open('config.ini.template') as f:
            conf_template = f.read()
        st.code(conf_template, language='ini')
    cols = st.columns([1,3])
    with cols[0]:
        if st.button('New JDBC driver', use_container_width=True):
            upload_jar_dialog()
    load_value('selected_jar')
    existing_jdbc = cols[1].multiselect('Select JDBC jar files', list_files('jdbc_jar'),
                                        key='_selected_jar', on_change=store_value, args=['selected_jar'],
                                        placeholder='Pick JDBC jar files here', label_visibility='collapsed')
    st.subheader('3. (optional) Advance parameters:')
    cols = st.columns(2)
    load_value('jvm_param')
    jvm_param = cols[0].text_input('JVM parameters', value='-Xmx4g',
                                   key='_jvm_param', on_change=store_value, args=['jvm_param'])
    load_value('java9')
    jdk9 = cols[0].checkbox('Java 9+', help='enable this will add "--add-opens=java.base/java.nio=ALL-UNNAMED" to jvm parameters',
                            key='_java9', on_change=store_value, args=['java9'])
    load_value('ignore_builtin_jdbc')
    no_default_jdbc = cols[0].checkbox('Ignore built-in clickzetta-java',
                                       help='do no include built-in clickzetta-java in classpath, in case you want to test with a version under development',
                                       key='_ignore_builtin_jdbc', on_change=store_value, args=['ignore_builtin_jdbc'])
    load_value('jobid_prefix')
    job_id_prefix = cols[1].text_input('job id prefix for clickzetta sql (optional)',
                                       help='if not specified, job id prefix will be empty',
                                       key='_jobid_prefix', on_change=store_value, args=['jobid_prefix'])
    load_value('stop_fail_rate')
    failure_rate = cols[1].slider('stop test if failure rate reach', 0, 100, 10, 1,
                                  help='test will stop if failure rate of sqls exceeds this value',
                                  key='_stop_fail_rate', on_change=store_value, args=['stop_fail_rate'])
    load_value('ping_count')
    ping_count = cols[1].number_input('ping packets for host check (0 disables)', value=10, min_value=0, step=1,
                                      help='ICMP packet count (ping -c N) for the pre-test host-reachability check; 0 skips it entirely',
                                      key='_ping_count', on_change=store_value, args=['ping_count'])

in_running_state = 'running_pid' in st.session_state and 'running_test' in st.session_state

with col_load_and_log:
    st.subheader('4. Run')
    cols = st.columns([1,1,2])
    run = cols[0].button('RUN', # on_click=clear_for_run,
                         use_container_width=True)
    stop = cols[1].button('STOP', use_container_width=True)

    duration_col = st.selectbox('select duration type', ['client_duration_ms', 'server_duration_ms'])

    log_container = st.container(height=500, border=False)
    with log_container:
        status = st.status('Ready to run test')
        with status:
            stdout = st.empty()

if stop and in_running_state:
    test = st.session_state['running_test']
    pid = st.session_state.pop('running_pid')
    os.kill(pid, signal.SIGTERM)
    msg = f'Stopped test {test}, pid {pid}'
    status.update(label=msg, state='error')
    st.toast(msg)
    st.session_state['last_run_test'] = test
    try:
        pid_file = os.path.join('data', test, 'pid')
        os.remove(pid_file)
    except:
        pass

test = None
csv = None

if in_running_state: # resume last test
    test = st.session_state['running_test']
    pid_file = os.path.join('data', test, 'pid')
    log_file = os.path.join('data', test, 'log.txt')
    csv_file = os.path.join('data', test, 'data.csv')
    with open(pid_file, 'r') as f:
        pid = int(f.read())
        st.session_state['running_pid'] = pid
    status.update(label=f'Continue running: {test}', state='running')
    log_thread = Thread(target=monitor_and_display_log, args=(log_file,))
    add_script_run_ctx(log_thread)
    log_thread.start()
    try:
        os.waitpid(pid, 0)
    except:
        pass
    status.update(label=f'Finished test {test}', state='complete')
    try:
        os.remove(pid_file)
    except:
        pass
    if 'running_pid' in st.session_state:
        st.session_state.pop('running_pid')
    if 'running_test' in st.session_state:
        _test = st.session_state.pop('running_test')
        st.toast(f'Test {_test} finished')
    st.session_state['last_run_test'] = test
    load_and_display_log(log_file)
    csv = csv_file
elif run:
    conf_path = existing_conf
    sql_paths = existing_sqls
    sql_path = ','.join([p for p in sql_paths if p])
    jdbc_path = ':'.join(existing_jdbc)
    if jdk9:
        jvm_param = f'--add-opens=java.base/java.nio=ALL-UNNAMED {jvm_param}'
    if not conf_path:
        stdout.error('please select a config file')
    elif not sql_path:
        stdout.error('please select sql files')
    else:
        now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        conf = conf_path.split(os.sep)[1].split(".")[0]
        test = f'{now}_{conf}'
        st.session_state['running_test'] = test
        test_folder = os.path.join('data', test)
        os.mkdir(test_folder)
        output_csv = os.path.join(test_folder, 'data.csv')
        output_log = os.path.join(test_folder, 'log.txt')
        pid_file = os.path.join(test_folder, 'pid')
        classpath = [ 'jdbc-stress-tool-1.0-jar-with-dependencies.jar' ]
        if not no_default_jdbc:
            classpath.append(CLICKZETTA_DRIVER)
        if existing_jdbc:
            classpath += existing_jdbc
        cmd = f'java {jvm_param}' + \
              f' -cp {":".join(classpath)}' + \
              ' com.clickzetta.jdbc_stress_tool.Main' + \
              f' -c {conf_path}' + \
              f' -q {sql_path}' + \
              f' -r {str(repeat)}' + \
              f' -t {str(thread)}' + \
              f' -f {str(failure_rate)}' + \
              f' --ping {str(ping_count)}' + \
              f' -o {output_csv}'
        if job_id_prefix != "":
            cmd += f' --prefix {job_id_prefix}'
        status.update(label=f'Runing: {test}\n\n{cmd}', state='running')
        log = open(output_log, 'w')
        process = subprocess.Popen(cmd.split(), stdout=log, stderr=subprocess.STDOUT)
        with open(pid_file, 'w') as f:
            f.write(str(process.pid))
        st.session_state['running_pid'] = process.pid
        st.toast(f'Run new test {test}, pid {process.pid}')
        log_thread = Thread(target=monitor_and_display_log, args=(output_log,))
        add_script_run_ctx(log_thread)
        log_thread.start()
        process.wait()
        log.close()
        status.update(label=f'Finished: {test}\n\n{cmd}', state='complete')
        try:
            os.remove(pid_file)
        except:
            pass
        if 'running_pid' in st.session_state:
            st.session_state.pop('running_pid')
        if 'running_test' in st.session_state:
            _test = st.session_state.pop('running_test')
            st.toast(f'Test {_test} finished')
        st.session_state['last_run_test'] = test
        load_and_display_log(output_log)
        csv = output_csv
elif 'last_run_test' in st.session_state: # display report of last test
    test = st.session_state['last_run_test']
    log_file = os.path.join('data', test, 'log.txt')
    csv_file = os.path.join('data', test, 'data.csv')
    status.update(label=f'Load last test: {test}', state='complete')
    load_and_display_log(log_file)
    csv = csv_file
    pass

cols = st.columns(2)
if cols[1].button(":rainbow[Go to view page to explore and manage test data]"):
    st.switch_page('view.py')

df = None
if csv:
    cols[0].subheader(f'Report of test {test}')
    try:
        df = pd.read_csv(csv)
    except Exception as ex:
        st.warning(f'Failed to read {csv}, reason {ex}')

if df is not None:
    render_report(df, duration_col)
