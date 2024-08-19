import pandas as pd
import numpy as np
import streamlit as st
from PIL import Image
import os
import datetime
import shutil
import altair as alt
from pathlib import Path

icon = None
try:
    icon = Image.open('icon.png')
except:
    pass

st.set_page_config(
    page_title="JDBC Stress Test Data Viewer",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/jdbc-stress-tool'
    }
)

st.title('JDBC Stress Test Data Viewer')
RENDER_LIMIT = 2000
selected_test = None

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'P{}'.format(n)
    return percentile_

def list_folders(folder):
    ret = []
    for f in os.scandir(folder):
        if f.is_dir():
            ret.append([f.name, datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')])
    ret.sort(key=lambda x: x[1], reverse=True)
    return ret

def save_file(file, folder) -> str:
    if file:
        dest = Path(f"{folder}/{file.name}")
        dest.write_bytes(file.read())
        return f'{folder}/{dest.name}'
    return None

def clear_test(_test):
    if os.path.exists(f'data/{_test}'):
        shutil.rmtree(f'data/{_test}')
    if os.path.exists(f'download/{_test}.zip'):
        os.remove(f'download/{_test}.zip')
    clear_value('view_selected_test')
    # st.rerun()

@st.dialog("Upload zip file")
def upload_dialog():
    cols = st.columns(2)
    cols[0].markdown(f'to `download/`, and unzipped to `data/`')
    staged = st.file_uploader(f'To folder download',
                              label_visibility='collapsed', type=['zip'])
    if st.button('OK', use_container_width=True):
        dest_path = 'download'
        if not os.path.exists(dest_path):
            os.mkdir(dest_path)
        uploaded = save_file(staged, dest_path)
        test_name = staged.name[:-4]
        unzip_path = f'data/{test_name}'
        os.mkdir(unzip_path)
        shutil.unpack_archive(uploaded, unzip_path)
        st.session_state['view_selected_test'] = test_name
        st.rerun()

@st.dialog("Rename test")
def rename_test(_test):
    st.markdown(f'`{_test}` to new name')
    _new = st.text_input('New test name', label_visibility='collapsed')
    if st.button('OK', use_container_width=True):
        if _new != _test:
            try:
                if os.path.exists(f'data/{_test}/{_test}.log'):
                    os.rename(f'data/{_test}/{_test}.log', f'data/{_test}/log.txt')
                if os.path.exists(f'data/{_test}/{_test}.csv'):
                    os.rename(f'data/{_test}/{_test}.csv', f'data/{_test}/data.csv')
                os.rename(f'data/{_test}', f'data/{_new}')
            except:
                pass
        st.rerun()

def store_value(key):
    st.session_state[key] = st.session_state["_"+key]
def load_value(key):
    if key in st.session_state:
        st.session_state["_"+key] = st.session_state[key]

def clear_value(key):
    if key in st.session_state:
        st.session_state.pop(key)
        st.session_state.pop("_"+key)

cols = st.columns([1,4])
with cols[0]:
    if st.button('New data file', use_container_width=True):
        upload_dialog()

    tests = list_folders('data')
    with st.container(height=500):
        load_value('view_selected_test')
        _tests = [t[0] for t in tests]
        if 'view_selected_test' in st.session_state and st.session_state['view_selected_test'] not in _tests:
            clear_value('view_selected_test')
        selected_test = st.radio(f'{len(tests)} tests', _tests, captions=[t[1] for t in tests],
                                 key='_view_selected_test', on_change=store_value, args=["view_selected_test"])

if selected_test:
    pid_file = f'data/{selected_test}/pid'
    log_file = f'data/{selected_test}/log.txt'
    csv_file = f'data/{selected_test}/data.csv'
    if os.path.exists(f'data/{selected_test}/{selected_test}.log'):
        os.rename(f'data/{selected_test}/{selected_test}.log', log_file)
    if os.path.exists(f'data/{selected_test}/{selected_test}.csv'):
        os.rename(f'data/{selected_test}/{selected_test}.csv', csv_file)
    log_title = 'Test log'
    if os.path.exists(pid_file): # test is still running
        log_title = f'Test log (still running)'
    with cols[1]:

        header_cols = st.columns([3,1,1,1])
        header_cols[0].subheader(f'Log of {selected_test}')
        data_file = f'download/{selected_test}.zip'
        if not os.path.exists(data_file):
            shutil.make_archive(f'download/{selected_test}', 'zip', f'data/{selected_test}/', '.')
        with open(data_file, 'rb') as f:
            header_cols[1].download_button('Download', f, file_name=f'{selected_test}.zip',
                                           mime='application/zip', use_container_width=True)
        if header_cols[2].button('Rename', use_container_width=True):
            rename_test(selected_test)
        header_cols[3].button('Delete', on_click=clear_test, args=(selected_test,), use_container_width=True)

        with st.container(height=500, border=False):
            with st.expander('Log', expanded=True):
                with open(log_file) as f:
                    log = f.read()
                    st.text(log)


    df = None
    if csv_file:
        cols = st.columns([4,1])
        cols[0].subheader(f'Report of test {selected_test}')
        duration_col = cols[1].selectbox('select duration type', ['client_duration_ms', 'server_duration_ms'])
        try:
            df = pd.read_csv(csv_file)
        except Exception as ex:
            st.warning(f'Failed to read {csv_file}, reason {ex}')

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
