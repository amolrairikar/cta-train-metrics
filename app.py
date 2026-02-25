"""
Streamlit app entry point.
"""

import streamlit as st

st.set_page_config(layout="wide")

pages = [
    st.Page(
        "pages/table_of_contents.py", title="Table of Contents", icon=":material/list:"
    ),
    st.Page("pages/schedule.py", title="Schedule", icon=":material/calendar_month:"),
    st.Page("pages/about.py", title="About", icon=":material/help_outline:"),
]

page = st.navigation(pages)
page.run()
