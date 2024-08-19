import streamlit as st
from PIL import Image

icon = None
try:
    icon = Image.open('icon.png')
except:
    pass

st.set_page_config(
    page_title="JDBC Stress Test Runner",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/jdbc-stress-tool'
    }
)

# Reducing whitespace on the top of the page
st.markdown("""
<style>

.block-container
{
    padding-top: 0.5rem;
    padding-bottom: 0rem;
    margin-top: 0.5rem;
}

</style>
""", unsafe_allow_html=True)

st.sidebar.title('JDBC Stress Test Tool')
st.sidebar.markdown('''This is a general purpose JDBC stress test tool, developed by [Singdata](https://www.singdata.com)

1. Construct a stress test with built-in or uploaded SQLs
1. Choose a target database with uploaded JDBC driver
1. Run and generate test data
1. Visualize and manage test data

This tool is open-sourced under Apache licence

- [Github](https://github.com/clickzetta/clickzetta-jdbc-stress-tool)
- [dockerhub](https://hub.docker.com/r/clickzetta/jdbc-stress-tool/tags)
''', unsafe_allow_html=True)

pg = st.navigation([
    st.Page("run.py", title="Run test", icon=":material/play_arrow:"),
    st.Page("view.py", title="View test", icon=":material/data_thresholding:"),
])

pg.run()