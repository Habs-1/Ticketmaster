import streamlit as st
from datetime import date
from data_loader import get_unique_values
# date.today()   -- For using today as a date later

def sidebar_controls():
    st.sidebar.header("Filter Options")
    
    start_date = st.sidebar.date_input("Start Date", date(2024, 1, 1))
    end_date = st.sidebar.date_input("End Date", date(2024, 1, 31))
        
    state = st.sidebar.text_input("State (optional)", help="Do not use acronyms. Case insensitive", placeholder="New York")
    city = st.sidebar.text_input("City (optional)", help="Case insensitive", placeholder="Buffalo")

    event_type = st.sidebar.multiselect(
    "Event Type",
    options = ["All", "Music", "Miscellaneous", "Arts & Theatre", "Sports", "Film"],
    default = ["All"]
    )

    min_price, max_price = st.sidebar.slider(
        "Price Range (in USD)", 
        min_value = 0, 
        max_value = 1000, 
        value=(0, 1000),
        step = 1,
        help="Setting minimum price to $0 will include all events without a listed price."
    )
   

    refresh_button = st.sidebar.button("Refresh Data")
    
    return start_date, end_date, event_type, state, city, min_price, max_price, refresh_button

