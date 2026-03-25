import pandas as pd
import numpy as np
import streamlit as st
import os
import datetime
import shutil
import altair as alt
from pathlib import Path
from report import render_report, RENDER_LIMIT

st.title('JDBC Stress Test Data Viewer')
selected_test = None

def list_folders(folder):
    ret = []
    for f in os.scandir(folder):
        if f.is_dir():
            ret.append([f.name, datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')])
    ret.sort(key=lambda x: x[1], reverse=True)
    return ret

def save_file(file, folder) -> str:
    if file:
        dest = Path(folder) / file.name
        dest.write_bytes(file.read())
        return os.path.join(folder, dest.name)
    return None

@st.dialog("Upload zip file")
def upload_zip_dialog():
    st.markdown(f'to `download/`, and unzipped to `data/`')
    staged = st.file_uploader(f'To folder download',
                              label_visibility='collapsed', type=['zip'])
    if st.button('OK', use_container_width=True):
        dest_path = 'download'
        if not os.path.exists(dest_path):
            os.mkdir(dest_path)
        uploaded = save_file(staged, dest_path)
        test_name = staged.name[:-4]
        unzip_path = os.path.join('data', test_name)
        os.mkdir(unzip_path)
        shutil.unpack_archive(uploaded, unzip_path)
        st.session_state['view_selected_test'] = test_name
        st.rerun()

@st.dialog("Rename test")
def rename_test(_test):
    st.markdown(f'`{_test}` to new name')
    _new = st.text_input('New test name', value=_test, label_visibility='collapsed')
    if st.button('OK', use_container_width=True):
        if _new and _new != _test:
            try:
                p = os.path.join('data', _test, f'{_test}.log')
                if os.path.exists(p):
                    os.rename(p, os.path.join('data', _test, 'log.txt'))
                p = os.path.join('data', _test, f'{_test}.csv')
                if os.path.exists(p):
                    os.rename(p, os.path.join('data', _test, 'data.csv'))
                os.rename(os.path.join('data', _test), os.path.join('data', _new))
            except Exception as e:
                st.error(f'Failed to rename: {e}')
            st.rerun()

@st.dialog("Delete test")
def delete_test(_test):
    st.markdown(f'Are you sure to delete `{_test}`?')
    if st.button('OK', use_container_width=True):
        p = os.path.join('data', _test)
        if os.path.exists(p):
            shutil.rmtree(p)
        p = os.path.join('download', f'{_test}.zip')
        if os.path.exists(p):
            os.remove(p)
        clear_value('view_selected_test')
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

cols = st.columns([1,3])
with cols[0]:
    if st.button('New data file', use_container_width=True):
        upload_zip_dialog()

    tests = list_folders('data')
    with st.container(height=500):
        load_value('view_selected_test')
        _tests = [t[0] for t in tests]
        if 'view_selected_test' in st.session_state and st.session_state['view_selected_test'] not in _tests:
            clear_value('view_selected_test')
        selected_test = st.radio(f'{len(tests)} tests', _tests, captions=[t[1] for t in tests],
                                 key='_view_selected_test', on_change=store_value, args=["view_selected_test"])

if selected_test:
    pid_file = os.path.join('data', selected_test, 'pid')
    log_file = os.path.join('data', selected_test, 'log.txt')
    csv_file = os.path.join('data', selected_test, 'data.csv')
    p = os.path.join('data', selected_test, f'{selected_test}.log')
    if os.path.exists(p):
        os.rename(p, log_file)
    p = os.path.join('data', selected_test, f'{selected_test}.csv')
    if os.path.exists(p):
        os.rename(p, csv_file)
    with cols[1]:
        header_cols = st.columns([4,1,1,1])
        header_cols[0].subheader(f'Test log of {selected_test}')
        data_file = os.path.join('download', f'{selected_test}.zip')
        data_dir = os.path.join('data', selected_test)
        if not os.path.exists(data_file) or os.path.getmtime(data_dir) > os.path.getmtime(data_file):
            shutil.make_archive(os.path.join('download', selected_test), 'zip', data_dir, '.')
        with open(data_file, 'rb') as f:
            header_cols[1].download_button('Download', f, file_name=f'{selected_test}.zip',
                                           mime='application/zip', use_container_width=True)
        if header_cols[2].button('Rename', use_container_width=True):
            rename_test(selected_test)
        if header_cols[3].button('Delete', use_container_width=True):
            delete_test(selected_test)

        with st.container(height=500, border=False):
            log_title = ':blue[Test finished]'
            if os.path.exists(pid_file): # test is still running
                log_title = f':red[Test is still running]'
            with st.expander(log_title, expanded=True):
                with open(log_file) as f:
                    log = f.read()
                    st.text(log)

    df = None
    if csv_file:
        cols = st.columns([4,1], vertical_alignment='bottom')
        cols[0].subheader(f'Report of test {selected_test}')
        duration_col = cols[1].selectbox('select duration type', ['client_duration_ms', 'server_duration_ms'])
        try:
            df = pd.read_csv(csv_file)
        except Exception as ex:
            st.warning(f'Failed to read {csv_file}, reason {ex}')

        if df is not None:
            render_report(df, duration_col)
