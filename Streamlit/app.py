import streamlit as st
from data_loader import query_snowflake
from sidebar import sidebar_controls
import pandas as pd

# Title and info
st.title("Ticketmaster Event Visualization")
st.write("Brandon Habschied bjh3420@rit.edu")

# Sidebar filters and refresh button
start_date, end_date, event_type, city, refresh_button = sidebar_controls()

# Tabs for the app and data reference
tab1, tab2 = st.tabs(["Event Dashboard", "Raw Dataset"])

# Cache function with 15-minute timeout
@st.cache_data(ttl=900)  # Cache data for 15 minutes
def cached_query(query: str) -> pd.DataFrame:
    return query_snowflake(query)

# Tab 1: Event Dashboard with Filters
with tab1:
    st.header("Filtered Events")
    
    # Dynamic SQL Query Construction
    def build_query(start_date, end_date, event_type, city):
        query = f"""
        SELECT * FROM "Events"
        WHERE "dates_start_localDate" BETWEEN '{start_date}' AND '{end_date}'"""    #start date filtering
        
        if event_type != "All":
            query += f""" AND "classifications_segment_name" = '{event_type}'"""    #event type filtering
        if city:
            query += f""" AND _embedded_venues_city_name = '{city}'"""            #city filtering
        
        query += " LIMIT 1000;"
        return query

    # Run query if the refresh button is clicked
    if refresh_button:
        try:
            query = build_query(start_date, end_date, event_type, city)
            data_df = cached_query(query)
            st.write("Filtered Events:")
            st.dataframe(data_df, height=500)
        except Exception as e:
            st.error(f"Error loading filtered data: {e}")
            st.write("Please check your filters or query parameters.")




# Tab 2: Raw Dataset for Reference
with tab2:
    st.header("Raw Dataset View")
    try:
        full_data_query = f"""
        SELECT * FROM "Events" LIMIT 1000;
        """
        full_data_df = cached_query(full_data_query)
        st.dataframe(full_data_df, height=500)
    except Exception as e:
        st.error(f"Error loading full dataset: {e}")
