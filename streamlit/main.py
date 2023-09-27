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
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridUpdateMode, GridOptionsBuilder, ExcelExportMode
import altair as alt
from PIL import Image

RENDER_LIMIT = 2000

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
    for path in ['conf', 'sql', 'jdbc_jar', 'data']:
        src = f'{vol}/{path}'
        if not os.path.exists(src):
            os.mkdir(src)
        if not os.path.exists(path):
            os.symlink(src, path)
else:
    for path in ['conf', 'sql', 'jdbc_jar', 'data']:
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
    return [''] + ret

def list_files(folder, filter=None):
    ret = ['']
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
        download_log.download_button('Download log', log, log_file, 'text/plain')
    except:
        pass

def analyze(csv:str):
    st.subheader('Report')
    df = None
    try:
        df = pd.read_csv(csv)
        with open(csv) as f:
            download_csv.download_button('Download csv', f, f'{csv}','text/csv')
    except Exception as ex:
        st.warning(f'failed to read {csv}, reason {ex}')
        return

    duration = df['client_end_ms'].max() - df['client_start_ms'].min()
    qps = 1000.0 * len(df) / duration
    st.code('current sql count {:,} \t time elapsed {:,} ms \t qps {:.3f}'.format(len(df), duration, qps))
    row_count = len(df)
    if row_count > RENDER_LIMIT:
        st.warning(f'too many data({row_count} rows), show aggregated chart only')

    df['overhead_ms'] = df['client_duration_ms'] - df['server_duration_ms']
    df['server_queue_ms'] = df['server_start_ms'] - df['server_submit_ms']
    df['server_exec_ms'] = df['server_end_ms'] - df['server_start_ms']
    df['gateway_overhead_ms'] = df['gateway_end_ms'] - df['gateway_start_ms'] - df['server_queue_ms'] - df['server_exec_ms']
    df['sdk_overhead_ms'] = df['client_duration_ms'] - (df['client_response_ms'] - df['client_request_ms'])
    df['network_ms'] = df['overhead_ms'] - df['gateway_overhead_ms'] - df['sdk_overhead_ms']

    if row_count <= RENDER_LIMIT: # detailed charts
        df_table = df[['thread_name', 'sql_id', 'job_id', 'is_success',
                'client_duration_ms', 'server_duration_ms',
                # 'overhead_ms', 'sdk_overhead_ms', 'gateway_overhead_ms', 'network_ms',
                'server_queue_ms', 'server_exec_ms']]
        AgGrid(df_table, height=400,
            use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
            excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
            enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

        # align timestamp to x-axis 0
        df['n_client_start_ms'] = df['client_start_ms'] - df['client_start_ms'].min()
        df['n_client_end_ms'] = df['client_end_ms'] - df['client_start_ms'].min()
        df['n_server_submit_ms'] = df['server_submit_ms'] - df['client_start_ms'].min()
        df['n_server_start_ms'] = df['server_start_ms'] - df['client_start_ms'].min()
        df['n_server_end_ms'] = df['server_end_ms'] - df['client_start_ms'].min()

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

    else: # charts for massive data
        df['t'] = pd.to_datetime(df['client_start_ms'], unit=f'ms')
        step = (df['client_start_ms'].max() - df['client_start_ms'].min()) // 300
        df_agg = df.groupby([pd.Grouper(key='t', freq=f'{step}ms'), 'sql_id']).agg({'client_duration_ms': ['mean', 'min', 'max']})
        df_agg.columns = df_agg.columns.map('.'.join)
        df_agg = df_agg.reset_index()
        df_agg.rename(columns={'t': 'client_start_time',
                               'client_duration_ms.mean': 'mean',
                               'client_duration_ms.min': 'min',
                               'client_duration_ms.max': 'max'}, inplace=True)

        c = alt.layer(
            alt.Chart(df_agg).mark_point(filled=False).encode(y=alt.Y('mean'), color='sql_id'),
            alt.Chart(df_agg).mark_errorbar().encode(y=alt.Y('min', title=None), y2='max', color='sql_id')
        ).encode(
            x=alt.X('client_start_time', title=None)
        ).interactive()
        st.altair_chart(c, use_container_width=True)

    # profiles
    st.subheader('6. Profile')
    stats = df.groupby('sql_id')['client_duration_ms'].agg(['count', 'min', 'max', 'mean', 'median'])
    stats['25%'] = df.groupby('sql_id')['client_duration_ms'].quantile(0.25)
    stats['75%'] = df.groupby('sql_id')['client_duration_ms'].quantile(0.75)
    stats['90%'] = df.groupby('sql_id')['client_duration_ms'].quantile(0.90)
    stats['95%'] = df.groupby('sql_id')['client_duration_ms'].quantile(0.95)
    stats['99%'] = df.groupby('sql_id')['client_duration_ms'].quantile(0.99)
    success_rate = df.groupby('sql_id')['is_success'].mean().rename('success_rate')
    stats = pd.merge(stats, success_rate, on='sql_id').reset_index()

    overall = df['client_duration_ms'].agg(['count', 'min', 'max', 'mean', 'median'])
    overall['25%'] = df['client_duration_ms'].quantile(0.25)
    overall['75%'] = df['client_duration_ms'].quantile(0.75)
    overall['90%'] = df['client_duration_ms'].quantile(0.90)
    overall['95%'] = df['client_duration_ms'].quantile(0.95)
    overall['99%'] = df['client_duration_ms'].quantile(0.99)
    overall['success_rate'] = df['is_success'].mean()
    overall = pd.DataFrame(overall).T
    overall['sql_id'] = '-- OVERALL --'

    stats = pd.concat([overall, stats], ignore_index=True)
    stats['success_rate'] = stats['success_rate'].apply(lambda x: round(x * 100, 2))

    AgGrid(stats[['sql_id', 'count', 'success_rate', 'min', '25%', 'median', 'mean', '75%', '90%', '95%', '99%', 'max']],
        use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
        excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
        enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

with col_conf_and_run:
    cols = st.columns([1,2])
    cols[0].subheader('1. Define stress:')
    cols[1].info('How many SQLs and JDBC concurrency')
    sqls = cols[0].file_uploader("Upload new sql file(s)", accept_multiple_files=True)
    all_sqls = list_files('sql')
    select_all_sqls = cols[1].checkbox('Select all', value=False)
    if select_all_sqls:
        existing_sqls = cols[1].multiselect('Choose existing sql files', all_sqls, [s for s in all_sqls if s])
    else:
        existing_sqls = cols[1].multiselect('Choose existing sql files', all_sqls)
    repeat = cols[1].number_input('Repeat times of SQLs', value=100, min_value=1, step=1)
    thread = cols[1].number_input('JDBC Concurrency', value=20, min_value=1, step=1)
    st.divider()
    cols = st.columns([1,2])
    cols[0].subheader('2. Define target:')
    cols[1].info('clickzetta-java included, NO need to upload')
    conf = cols[0].file_uploader("Upload new config file", accept_multiple_files=False)
    existing_conf = cols[1].selectbox('Choose existing config file', list_files('conf'))
    with cols[1].expander('Config file template'):
        with open('config.ini.template') as f:
            conf_template = f.read()
        st.code(conf_template, language='ini')
    cols = st.columns([1,2])
    jdbc = cols[0].file_uploader('Upload new JDBC Jar', accept_multiple_files=True)
    existing_jdbc = cols[1].multiselect('Choose existing jdbc jars', list_files('jdbc_jar'))
    st.divider()
    st.subheader('3. (optional) Define runtime parameters:')
    cols = st.columns([1,2])
    jvm_param = cols[0].text_input('JVM parameters', value='-Xmx4g')
    jdk9 = cols[0].checkbox('Java 9+', help='enable this will add "--add-opens=java.base/java.nio=ALL-UNNAMED" to jvm parameters')
    named_test = cols[1].text_input('name this test (optional)', help='if not specified, test output folder/files will be named in format of "$datetime_$config.csv"')
    job_id_prefix = cols[1].text_input('job id prefix for clickzetta sql (optional)', help='if not specified, job id prefix will be empty')
    failure_rate = cols[1].slider('stop test if failure rate reach', 0, 100, 10, 1, help='test will stop if failure rate of sqls exceeds this value')
    cols = st.columns(4)
    cols[0].subheader('4. Run test')
    run = cols[2].button('RUN')
    stop = cols[3].button('STOP')

if conf:
    uploaded = save_file(conf, 'conf')
    existing_conf = uploaded

if sqls:
    uploaded = save_files(sqls, 'sql')
    existing_sqls = uploaded

if jdbc:
    uploaded = save_files(jdbc, 'jdbc_jar')
    existing_jdbc = uploaded

with col_load_and_log:
    st.subheader('5. View logs OR load existing test')
    existing_test = st.selectbox('Load existing tests', list_folders('data'))
    st.divider()
    test_head = st.empty()
    stdout = st.empty()
    cols = st.columns(2)
    download_log = cols[0].empty()
    download_csv = cols[1].empty()

if stop and 'pid' in st.session_state and 'test' in st.session_state:
    test = st.session_state['test']
    pid = st.session_state.pop('pid')
    test_head.error(f'stop test {test}, pid {pid}')
    print(f'stop test {test}, pid {pid}')
    os.kill(pid, signal.SIGTERM)
    try:
        pid_file = f'data/{test}/{test}.pid'
        os.remove(pid_file)
    except:
        pass

if existing_test:
    test = existing_test
    st.session_state['test'] = test
    pid_file = f'data/{test}/{test}.pid'
    log_file = f'data/{test}/{test}.log'
    csv_file = f'data/{test}/{test}.csv'
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
        analyze(csv_file)
    else: # test finished
        test_head.info(f'Load existing test: {test}')
        load_and_display_log(log_file)
        analyze(csv_file)
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
        if not named_test:
            now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            conf = conf_path.split("/")[1].split(".")[0]
            named_test = f'{now}_{conf}'

        test_head.info(f'run new test : {named_test}')
        st.session_state['test'] = named_test
        test_folder = f'data/{named_test}'
        os.mkdir(test_folder)
        output_csv = f'{test_folder}/{named_test}.csv'
        output_log = f'{test_folder}/{named_test}.log'
        pid_file = f'{test_folder}/{named_test}.pid'
        cmd = f'java {jvm_param}' + \
              f' -cp jdbc-stress-tool-1.0-jar-with-dependencies.jar:clickzetta-java-1.0.2-jar-with-dependencies.jar:{jdbc_path}' + \
              ' com.clickzetta.jdbc_stress_tool.Main' + \
              f' -c {conf_path}' + \
              f' -q {sql_path}' + \
              f' -r {str(repeat)}' + \
              f' -t {str(thread)}' + \
              f' -f {str(failure_rate)}' + \
              f' -o {output_csv}'
        if job_id_prefix != "":
            cmd += f' --prefix {job_id_prefix}'
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
        analyze(output_csv)
