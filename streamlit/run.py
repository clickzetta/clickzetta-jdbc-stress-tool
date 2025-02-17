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

RENDER_LIMIT = 2000

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

def save_file(file, folder) -> str:
    if file:
        dest = Path(folder) / file.name
        dest.write_bytes(file.read())
        return os.path.join(folder, dest.name)
    return None

def save_files(files, folder) -> str:
    if files:
        dests = []
        for f in files:
            dests.append(save_file(f, folder))
        return dests
    return None

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

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'P{}'.format(n)
    return percentile_

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
        uploaded = save_file(staged, dest)
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
    thread = Thread(target=monitor_and_display_log, args=(log_file,))
    add_script_run_ctx(thread)
    thread.start()
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
        thread = Thread(target=monitor_and_display_log, args=(output_log,))
        add_script_run_ctx(thread)
        thread.start()
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
    duration = df['client_end_ms'].max() - df['client_start_ms'].min()
    qps = 1000.0 * len(df) / duration
    st.code('current sql count {:,} \t time elapsed {:,} ms \t qps {:.3f}'.format(len(df), duration, qps))
    row_count = len(df)
    step = duration // 300

    # align timestamp to x-axis 0
    df['n_client_start_ms'] = df['client_start_ms'] - df['client_start_ms'].min()
    df['n_client_end_ms'] = df['client_end_ms'] - df['client_start_ms'].min()
    df['n_server_submit_ms'] = df['server_submit_ms'] - df['client_start_ms'].min()
    df['n_server_start_ms'] = df['server_start_ms'] - df['client_start_ms'].min()
    df['n_server_end_ms'] = df['server_end_ms'] - df['client_start_ms'].min()

    df['overhead_ms'] = df['client_duration_ms'] - df['server_duration_ms']
    df['server_queue_ms'] = df['server_start_ms'] - df['server_submit_ms']
    df['server_exec_ms'] = df['server_end_ms'] - df['server_start_ms']
    df['gateway_overhead_ms'] = df['gateway_end_ms'] - df['gateway_start_ms'] - df['server_queue_ms'] - df['server_exec_ms']
    df['sdk_overhead_ms'] = df['client_duration_ms'] - (df['client_response_ms'] - df['client_request_ms'])
    df['network_ms'] = df['overhead_ms'] - df['gateway_overhead_ms'] - df['sdk_overhead_ms']

    if row_count >= RENDER_LIMIT:
        st.warning(f'too many data({row_count} rows), no detailed duration distribution chart')
    else: # detailed charts
        df_table = df[['thread_name', 'sql_id', 'job_id', 'is_success', 'result_size',
                'client_duration_ms', 'server_duration_ms',
                # 'overhead_ms', 'sdk_overhead_ms', 'gateway_overhead_ms', 'network_ms',
                'server_queue_ms', 'server_exec_ms']]
        st.markdown('### Duration table')
        st.dataframe(df_table, height=400, use_container_width=True, hide_index=True)

        hint = ['sql_id', 'job_id', 'n_client_start_ms', 'n_client_end_ms', 'client_duration_ms',
                'n_server_submit_ms', 'n_server_start_ms', 'n_server_end_ms', 'server_duration_ms',
                'server_queue_ms', 'server_exec_ms']

        c_client = alt.Chart().mark_bar().encode(
            x='n_client_start_ms',
            x2='n_client_end_ms',
            y='thread_name',
            detail=hint,
            color='sql_id'
        ).interactive()
        c_text = alt.Chart().mark_text(align='left', baseline='middle', color='white').encode(
            x=alt.X('n_client_start_ms'),
            y=alt.Y('thread_name'),
            text=alt.Text('client_duration_ms'),
            detail=hint,
        ).interactive()

        c = alt.layer(c_client, c_text, data=df)
        st.altair_chart(c, use_container_width=True)

    # duration(latency) chart
    st.markdown(f'#### Duration(Latency) Chart: {duration_col}')

    df_duration = df[['sql_id', 'n_client_end_ms', duration_col]]
    df_duration['time'] = df_duration['n_client_end_ms'] // step * step
    df_duration = df_duration.groupby(['time', 'sql_id']).agg(
        {duration_col: ['mean', 'min', 'max', percentile(90), percentile(95), percentile(99)]})
    df_duration.columns = df_duration.columns.map('.'.join)
    df_duration.rename(columns={
                            f'{duration_col}.P90': 'P90',
                            f'{duration_col}.P95': 'P95',
                            f'{duration_col}.P99': 'P99',
                            f'{duration_col}.mean': 'mean',
                            f'{duration_col}.min': 'min',
                            f'{duration_col}.max': 'max'}, inplace=True)
    df_duration = df_duration.reset_index()
    hint = ['time', 'min', 'mean', 'P90', 'P95', 'P99', 'max']
    c = alt.layer(
        alt.Chart(df_duration).mark_point(filled=False).encode(y=alt.Y('mean'), color='sql_id', detail=hint),
        alt.Chart(df_duration).mark_errorbar().encode(y=alt.Y('min', title='duration(ms)'), y2='max', color='sql_id', detail=hint)
    ).encode(
        x=alt.X('time', title='time(ms)')
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    # qps chart
    st.markdown('#### QPS Chart')
    df_qps = df[['n_client_end_ms', 'client_duration_ms']]
    qps_step = step if step >= 1000 else 1000
    df_qps['time'] = df_qps['n_client_end_ms'] // qps_step * qps_step / 1000
    df_qps = df_qps.groupby('time').agg({'client_duration_ms': ['count']})
    df_qps.columns = df_qps.columns.map('.'.join)
    df_qps.rename(columns={'client_duration_ms.count': 'count'}, inplace=True)
    df_qps['qps'] = df_qps['count'] * 1000 / qps_step
    df_qps = df_qps.reset_index()
    c = alt.layer(
        alt.Chart(df_qps).mark_line(point=True).encode(y=alt.Y('qps'))
    ).encode(
        x=alt.X('time', title='time(s)')
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    # profile dataframe
    st.markdown(f'#### SQL Profile Table: {duration_col}')
    stats = df.groupby('sql_id')[duration_col].agg(['count', 'min', 'max', 'mean', 'median'])
    stats['25%'] = df.groupby('sql_id')[duration_col].quantile(0.25)
    stats['75%'] = df.groupby('sql_id')[duration_col].quantile(0.75)
    stats['90%'] = df.groupby('sql_id')[duration_col].quantile(0.90)
    stats['95%'] = df.groupby('sql_id')[duration_col].quantile(0.95)
    stats['99%'] = df.groupby('sql_id')[duration_col].quantile(0.99)
    success_rate = df.groupby('sql_id')['is_success'].mean().rename('success_rate')
    stats = pd.merge(stats, success_rate, on='sql_id').reset_index()

    overall = df[duration_col].agg(['count', 'min', 'max', 'mean', 'median'])
    overall['25%'] = df[duration_col].quantile(0.25)
    overall['75%'] = df[duration_col].quantile(0.75)
    overall['90%'] = df[duration_col].quantile(0.90)
    overall['95%'] = df[duration_col].quantile(0.95)
    overall['99%'] = df[duration_col].quantile(0.99)
    overall['success_rate'] = df['is_success'].mean()
    overall = pd.DataFrame(overall).T
    overall['sql_id'] = '-- OVERALL --'

    stats = pd.concat([overall, stats], ignore_index=True)
    stats['success_rate'] = stats['success_rate'].apply(lambda x: round(x * 100, 2))

    st.dataframe(stats[['sql_id', 'count', 'success_rate', 'min', '25%', 'median', 'mean', '75%', '90%', '95%', '99%', 'max']],
                 use_container_width=True, hide_index=True)
