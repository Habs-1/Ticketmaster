import streamlit as st
from data_loader import query_snowflake
from sidebar import sidebar_controls
import pandas as pd

# Title and info

st.set_page_config(layout="wide")
st.title("Ticketmaster Event Visualization")
st.write("Brandon Habschied bjh3420@rit.edu")

minimal_view = ["name", "url", "classifications_segment_name", "classifications_genre_name", "priceRanges_min", "priceRanges_max", 
        "dates_start_localDate", "dates_start_localTime", "_embedded_venues_name", "_embedded_venues_state_name", 
        "_embedded_venues_city_name"]

# Sidebar filters and refresh button
start_date, end_date, event_type, state, city,  min_price, max_price, view_mode, refresh_button = sidebar_controls()

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
    def build_query(start_date, end_date, event_type, state, city, min_price, max_price):
        query = f"""
        SELECT * FROM "Events"
        WHERE "dates_start_localDate" BETWEEN '{start_date}' AND '{end_date}'"""    #start date filtering
        
        if event_type and "All" not in event_type:
            segment_list = "', '".join(event_type)
            query += f""" AND "CLASSIFICATIONS_SEGMENT_NAME" IN ('{segment_list}') """
        if state:
            query += f""" AND _embedded_venues_state_name ilike '{state}%'"""       #city filtering (caps insensitive and wildcard ending)
        if city:
            query += f""" AND _embedded_venues_city_name ilike '{city}%'"""         #city filtering (caps insensitive and wildcard ending)
        if min_price != 0:
            query += f""" AND "priceRanges_min" >= {min_price}"""                      # Minimum price filter
        else:
            query += f""" AND ("priceRanges_min" IS NULL OR "priceRanges_min" >= {min_price})""" 
        query += f""" AND "priceRanges_max" <= {max_price}"""                      # Maximum price filter
        
        # query += " LIMIT 1000;"
        return query

    # Run query if the refresh button is clicked
    if refresh_button:
        try:
            query = build_query(start_date, end_date, event_type, state, city, min_price, max_price)
            data_df = cached_query(query)
            num_events = len(data_df)
            st.write(f"Filtered Events: {num_events}")
            if view_mode == "Full View":
                st.dataframe(data_df, height = 500)
            else:
                st.subheader("Minimal View")
                st.dataframe(data_df[minimal_view])
            st.write("Generated Query: (for testing purposes)")
            st.write(query)
        except Exception as e:
            st.error(f"Error loading filtered data: {e}")
            st.write("Please check your filters or query parameters.")


# Tab 2: Raw Dataset for Reference
with tab2:
    st.header("Raw Dataset View")
    try:
        full_data_query = f"""
        SELECT * FROM "Events";
        """
        full_data_df = cached_query(full_data_query)
        num_events = len(full_data_df)
        st.write(f"Raw Data Events: {num_events}")
        st.dataframe(full_data_df, height=500, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading full dataset: {e}")
