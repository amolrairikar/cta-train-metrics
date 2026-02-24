"""
Streamlit app entry point.
"""

import streamlit as st

pages = [
    st.Page("pages/home.py", title="Home", icon=":material/home:"),
    st.Page("pages/schedule.py", title="Schedule", icon=":material/calendar_month:"),
]

page = st.navigation(pages)
page.run()
