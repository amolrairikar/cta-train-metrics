import streamlit as st

st.title("Table of Contents")

pages = [
    (
        "Schedule",
        "schedule",
        """
        View details about the planned "L" train schedule such as trains scheduled per hour, average 
        time distance (headway) between train runs per hour, and the historical trend of aggregate 
        scheduled trains to identify planned service increases/decreases over time.
        """,
    ),
    (
        "About",
        "about",
        """
        Find out more details about this project such as the background, methodology, total number 
        of trains tracked to-date, and more!
        """,
    ),
]

# Create headers
col1, col2 = st.columns([1, 3])
col1.subheader("Page")
col2.subheader("Description")

st.divider()

# Generate rows
for label, path, desc in pages:
    c1, c2 = st.columns([1, 3])
    with c1:
        st.page_link(f"pages/{path}.py", label=label)
    with c2:
        st.write(desc)
