import streamlit as st

pg = st.navigation([
    st.Page("run.py", title="Run test", icon=":material/play_arrow:"),
    st.Page("view.py", title="View test", icon=":material/data_thresholding:"),
])

pg.run()