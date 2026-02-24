import streamlit as st

st.title('Chicago "L" Train Service Analytics')

st.header("Background")

st.markdown(
    """
    The Chicago Transit Authority (CTA) publishes General Transit Feed Specification (GTFS) data
    containing information about scheduled "L" train service, and an Application Programming Interface (API)
    for querying live CTA train runs. Using this data, we can answer questions such as:
      - How often do trains arrive at my nearest station during rush hour?
      - How reliable is the "L" train service on weekends?
      - How many of the scheduled trains actually run?
      - Which "L" line has the fewest delays?

    And much more!
    """
)

st.header("Methodology")

st.markdown(
    """
    The CTA API has a Locations API [endpoint](https://www.transitchicago.com/developers/ttdocs/#locations) 
    that provides the location of all trains running on a given line at that moment. The application queries this 
    API every minute (per "L" line) to get the current snapshot of train locations. We can approximate when a train 
    arrived at a station by using the last `arrT` (arrival time) value before a new `nextStaNm` (next station name) 
    appears for that run. For example, if Blue Line run #100 has `arrT: 2026-02-24T13:24:17` for the Grand station and 
    the next minute has `arrT: 2026-02-24T13:26:17` for the Chicago station, we can assume that the train arrived 
    at the Grand station at 1:24pm. We can then compare these estimated arrivals to the scheduled "L" train service 
    constructed from GTFS data to assess whether a scheduled train service had a corresponding live train run.
    """
)

st.header("Credits")
st.write(
    """
    Data provided by Chicago Transit Authority. This application would not be possible without the CTA API service
    providing live snapshots of train locations and estimated arrivals.
    """
)
