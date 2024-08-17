import os
from datetime import datetime
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import signal
import time
import shutil
from io import StringIO
from threading import Thread
from contextlib import contextmanager, redirect_stdout
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridUpdateMode, GridOptionsBuilder, ExcelExportMode
import altair as alt
from PIL import Image

RENDER_LIMIT = 2000
CLICKZETTA_DRIVER = 'clickzetta-java-1.4.16.jar'

icon = None
try:
    icon = Image.open('icon.png')
except:
    pass

st.set_page_config(
    page_title="ClickZetta Lakehouse JDBC Stress Test",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/jdbc-stress-tool'
    }
)

st.title('ClickZetta Lakehouse JDBC Stress Tool')
col_conf_and_run, col_load_and_log = st.columns(2)

if 'VOLUME' in os.environ: # for docker
    vol = os.environ['VOLUME']
    for path in ['conf', 'sql', 'jdbc_jar', 'data', 'download']:
        src = f'{vol}/{path}'
        if not os.path.exists(src):
            os.mkdir(src)
        if not os.path.exists(path):
            os.symlink(src, path)
else:
    for path in ['conf', 'sql', 'jdbc_jar', 'data', 'download']:
        if not os.path.exists(path):
            os.mkdir(path)

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
        dest = Path(f"{folder}/{file.name}")
        dest.write_bytes(file.read())
        return f'{folder}/{dest.name}'
    return None

def save_files(files, folder) -> str:
    if files:
        dests = []
        for f in files:
            dests.append(save_file(f, folder))
        return dests
    return None

def list_folders(folder):
    ret = []
    for f in os.scandir(folder):
        if f.is_dir():
            ret.append(f.name)
    ret.sort(key=lambda x: os.path.getmtime(f'{folder}/{x}'), reverse=True)
    return ret

def list_files(folder, filter=None):
    ret = []
    files = os.listdir(folder)
    if files:
        files.sort(key=lambda x: os.path.getmtime(f'{folder}/{x}'), reverse=True)
        for f in files:
            if not filter or (filter and f.endswith(filter)):
                ret.append(f'{folder}/{f}')
    return ret

def monitor_and_display_log(filename):
    try:
        with open(filename) as f, st_capture(stdout.code):
            f.seek(0,2)
            while 'pid' in st.session_state:
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
        stdout.code(log)
    except:
        pass

# 定义一个函数来计算 P95 和 P99
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'P{}'.format(n)
    return percentile_

def clear_for_run():
    if 'select_test' in st.session_state:
        st.session_state.pop('select_test')
    if 'test' in st.session_state:
        st.session_state.pop('test')

csv = None

with col_conf_and_run:
    st.subheader('1. Define stress: SQLs and concurreny')
    cols = st.columns([1,2])
    uploaded_sqls = None
    with cols[0].form('upload_sql', clear_on_submit=True):
        sqls = st.file_uploader("Upload new sql file(s)", accept_multiple_files=True)
        submitted = st.form_submit_button("UPLOAD")
        if submitted and sqls is not None:
            uploaded = save_files(sqls, 'sql')
            st.session_state['selected_sqls'] = uploaded
    all_sqls = list_files('sql')
    tpc_h = list_files('benchmark/tpc-h')
    ssb_flat = list_files('benchmark/ssb-flat')
    sql_template = cols[1].selectbox('Select pre-defined benchmark', ['tpc-h', 'ssb-flat', 'all uploaded sql'],
                                     index=None, placeholder='Pick pre-defined benchmark here')
    # select_all_sqls = cols[1].checkbox('Select all', value=False)
    if sql_template == 'tpc-h':
        st.session_state['selected_sqls'] = tpc_h
    elif sql_template == 'ssb-flat':
        st.session_state['selected_sqls'] = ssb_flat
    if sql_template == 'all uploaded sql':
        st.session_state['selected_sqls'] = all_sqls
    selected_count = ''
    if st.session_state.get('selected_sqls'):
        selected_count = f'({len(st.session_state.get("selected_sqls"))} files)'
    existing_sqls = cols[1].multiselect(f'Select sql files {selected_count}',
                                        all_sqls + tpc_h + ssb_flat,
                                        st.session_state.get('selected_sqls'),
                                        placeholder='Pick SQL files here')
                                        # format_func=lambda x: f'...{x[-10:]}' if len(x) > 13 else x)
    repeat = cols[1].number_input('Repeat times of SQLs', value=100, min_value=1, step=1)
    thread = cols[1].number_input('JDBC Concurrency', value=20, min_value=1, step=1)
    st.divider()
    st.subheader('2. Define target: config and driver')
    cols = st.columns([1,2])
    with cols[0].form('upload_conf', clear_on_submit=True):
        conf = st.file_uploader("Upload new config file")
        submitted = st.form_submit_button("UPLOAD")
        if submitted and conf is not None:
            uploaded = save_file(conf, 'conf')
            st.session_state['selected_conf'] = uploaded
    all_confs = list_files('conf')
    idx = None
    if st.session_state.get('selected_conf'):
        idx = all_confs.index(st.session_state.get('selected_conf'))
    existing_conf = cols[1].selectbox('Select config file', all_confs, idx, placeholder='Pick config file here')
    with cols[1].expander('Config file template'):
        with open('config.ini.template') as f:
            conf_template = f.read()
        st.code(conf_template, language='ini')
    cols = st.columns([1,2])
    with cols[0].form('upload_jdbc', clear_on_submit=True):
        jdbc = st.file_uploader('Upload new JDBC Jar', type=['jar', 'zip'], accept_multiple_files=True)
        submitted = st.form_submit_button("UPLOAD")
        if submitted and conf is not None:
            uploaded = save_files(jdbc, 'jdbc_jar')
            st.session_state['selected_jar'] = uploaded
    cols[1].warning('No need to upload or select clickzetta-java')
    existing_jdbc = cols[1].multiselect('Select JDBC jar files', list_files('jdbc_jar'), st.session_state.get('selected_jar'),
                                        placeholder='Pick JDBC jar files here')
    st.divider()
    st.subheader('3. (optional) Advance parameters:')
    cols = st.columns([1,2])
    jvm_param = cols[0].text_input('JVM parameters', value='-Xmx4g')
    jdk9 = cols[0].checkbox('Java 9+', help='enable this will add "--add-opens=java.base/java.nio=ALL-UNNAMED" to jvm parameters')
    no_default_jdbc = cols[0].checkbox('Ignore built-in clickzetta-java',
                                       help='do no include built-in clickzetta-java in classpath, in case you want to test with a version under development')
    job_id_prefix = cols[1].text_input('job id prefix for clickzetta sql (optional)', help='if not specified, job id prefix will be empty')
    failure_rate = cols[1].slider('stop test if failure rate reach', 0, 100, 10, 1, help='test will stop if failure rate of sqls exceeds this value')

with col_load_and_log:
    st.subheader('4. Run new test or view existing test')
    cols = st.columns(2)
    run = cols[0].button('RUN', on_click=clear_for_run)
    stop = cols[1].button('STOP')
    st.divider()
    cols = st.columns(2)
    tests = [''] + list_folders('data')
    idx = None
    try:
        if st.session_state.get('select_test'):
            idx = tests.index(st.session_state.get('select_test'))
        elif st.session_state.get('rename_test'):
            idx = tests.index(st.session_state.get('rename_test'))
        elif st.session_state.get('test'):
            idx = tests.index(st.session_state.get('test'))
    except:
        pass
    existing_test = cols[0].selectbox('Select test', tests, idx, placeholder='Pick test here', key='select_test')
    duration_col = cols[0].selectbox('select duration type', ['client_duration_ms', 'server_duration_ms'])
    with cols[1].form("upload_zip", clear_on_submit=True):
        zip = st.file_uploader("Upload zip file", type=['zip'])
        submitted = st.form_submit_button("UPLOAD")
        if submitted and zip is not None:
            test = zip.name[:-4]
            zip_file = save_file(zip, 'download')
            os.mkdir(f'data/{test}')
            shutil.unpack_archive(zip_file, f'data/{test}')
            st.session_state['test'] = test
            st.rerun()

    st.divider()
    test_head = st.empty()
    with st.expander('Show test log', expanded=True):
        stdout = st.empty()
    st.divider()
    download_head = st.empty()
    download = st.empty()

def clear_test(_test):
    if os.path.exists(f'data/{_test}'):
        shutil.rmtree(f'data/{_test}')
    if os.path.exists(f'download/{_test}.zip'):
        os.remove(f'download/{_test}.zip')
    if 'test' in st.session_state:
        st.session_state.pop('test')
    if 'rename_test' in st.session_state:
        st.session_state.pop('rename_test')
    if 'select_test' in st.session_state:
        st.session_state.pop('select_test')

def prepare_download_btn(_test):
    if os.path.exists(f'download/{_test}'):
        return
    download_head.subheader('6. Rename, download or drop test data')
    cols = download.columns([2,1,1])
    cols[0].text_input('Rename this test', value=_test, key='rename_test', label_visibility='collapsed')
    data_file = f'download/{_test}.zip'
    if not os.path.exists(data_file):
        shutil.make_archive(f'download/{_test}', 'zip', f'data/{_test}/', '.')
    with open(data_file, 'rb') as f:
        cols[1].download_button('Download data as zip', f, f'{_test}.zip', 'application/zip')
    cols[2].button('Drop this test', on_click=clear_test, args=(_test,))

if stop and 'pid' in st.session_state and 'test' in st.session_state:
    test = st.session_state['test']
    pid = st.session_state.pop('pid')
    test_head.error(f'stop test {test}, pid {pid}')
    print(f'stop test {test}, pid {pid}')
    os.kill(pid, signal.SIGTERM)
    try:
        pid_file = f'data/{test}/pid'
        os.remove(pid_file)
    except:
        pass

if st.session_state.get('rename_test') and st.session_state.get('test'):
    src = st.session_state.pop('test').strip()
    dest = st.session_state.pop('rename_test').strip()
    if src != dest:
        try:
            if os.path.exists(f'data/{src}/{src}.log'):
                os.rename(f'data/{src}/{src}.log', f'data/{src}/log.txt')
            if os.path.exists(f'data/{src}/{src}.csv'):
                os.rename(f'data/{src}/{src}.csv', f'data/{src}/data.csv')
            os.rename(f'data/{src}', f'data/{dest}')
            test_head.info(f'renamed test "{src}" to "{dest}"')
            st.session_state.pop('rename_test')
        except:
            pass
    st.session_state['test'] = dest
    st.rerun()

if existing_test:
    test = existing_test
    st.session_state['test'] = test
    pid_file = f'data/{test}/pid'
    log_file = f'data/{test}/log.txt'
    csv_file = f'data/{test}/data.csv'
    if os.path.exists(f'data/{test}/{test}.log'):
        os.rename(f'data/{test}/{test}.log', log_file)
    if os.path.exists(f'data/{test}/{test}.csv'):
        os.rename(f'data/{test}/{test}.csv', csv_file)
    if os.path.exists(pid_file): # test is still running
        with open(pid_file, 'r') as f:
            pid = int(f.read())
            st.session_state['pid'] = pid
        test_head.info(f'Test is still running: {test}')
        thread = Thread(target=monitor_and_display_log, args=(log_file,))
        add_script_run_ctx(thread)
        thread.start()
        try:
            os.waitpid(pid, 0)
        except:
            pass
        try:
            os.remove(pid_file)
        except:
            pass
        if 'pid' in st.session_state:
            st.session_state.pop('pid')
        load_and_display_log(log_file)
        prepare_download_btn(test)
        csv = csv_file
    else: # test finished
        test_head.info(f'Load existing test: {test}')
        load_and_display_log(log_file)
        prepare_download_btn(test)
        csv = csv_file
elif run and 'pid' not in st.session_state:
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
        conf = conf_path.split("/")[1].split(".")[0]
        test = f'{now}_{conf}'
        st.session_state['test'] = test
        test_folder = f'data/{test}'
        os.mkdir(test_folder)
        output_csv = f'{test_folder}/data.csv'
        output_log = f'{test_folder}/log.txt'
        pid_file = f'{test_folder}/pid'
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
        test_head.code(f'run new test : {test}\n\n{cmd}')
        log = open(output_log, 'w')
        process = subprocess.Popen(cmd.split(), stdout=log, stderr=subprocess.STDOUT)
        with open(pid_file, 'w') as f:
            f.write(str(process.pid))
        st.session_state['pid'] = process.pid
        thread = Thread(target=monitor_and_display_log, args=(output_log,))
        add_script_run_ctx(thread)
        thread.start()
        process.wait()
        log.close()
        try:
            os.remove(pid_file)
        except:
            pass
        if 'pid' in st.session_state:
            st.session_state.pop('pid')
        load_and_display_log(output_log)
        prepare_download_btn(test)
        csv = output_csv

df = None
if csv:
    st.subheader(f'Report of test {st.session_state.get("test")}')
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
