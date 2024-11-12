import streamlit as st
from datetime import date
# date.today()   -- For using today as a date later

def sidebar_controls():
    st.sidebar.header("Filter Options")
    
    #Example filter options
    start_date = st.sidebar.date_input("Start Date", date(2024, 1, 1))
    end_date = st.sidebar.date_input("End Date", date(2024, 1, 31))
    
    event_type = st.sidebar.selectbox("Event Type", ["All", "Concert", "Sports", "Theater", "Comedy"])
    city = st.sidebar.text_input("City (optional)")

    #Refresh Button
    refresh_button = st.sidebar.button("Refresh Data")
    
    return start_date, end_date, event_type, city, refresh_button

