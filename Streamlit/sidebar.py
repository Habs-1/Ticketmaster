import streamlit as st
from datetime import date
from data_loader import get_unique_values
# date.today()   -- For using today as a date later

def sidebar_controls():
    st.sidebar.header("Filter Options")
    
    start_date = st.sidebar.date_input("Start Date", date(2024, 1, 1))
    end_date = st.sidebar.date_input("End Date", date(2024, 1, 31))
    
    event_types = ["All", "Music", "Miscellaneous", "Arts & Theatre", "Sports", "Film"]
    event_type = st.sidebar.selectbox("Select Event Type", options=event_types, index=0)
    
    state = st.sidebar.text_input("State (optional)")
    city = st.sidebar.text_input("City (optional)")

    refresh_button = st.sidebar.button("Refresh Data")
    
    return start_date, end_date, event_type, state, city, refresh_button

