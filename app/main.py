import streamlit as st
import time

# FUNCTIONALITY
def reset_app():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    
    # Cache reset
    st.cache_data.clear()
    st.cache_resource.clear()

    st.success("Application state has been reset. Refreshing")
    time.sleep(1)
    st.rerun()
# ---------------------------------------- #


st.set_page_config(
    page_title="Home",
    page_icon="assets/tower_bw.svg",
)
# st.logo("assets/tower_bw.svg")

# --------- PAGE CONTENT --------- #
db_automation_page = st.Page(
    page = "services/Automate DB IOH.py",
    title = "DB IOH Automation",
    icon= ":material/home_storage:",
)
kml_renamer_page = st.Page(
    page = "services/KML Renamer.py",
    title = "KML Renamer",
    icon= ":material/app_registration:",
)

pg = st.navigation([db_automation_page, kml_renamer_page])
pg.run()

# FOOTER
st.markdown("---")
footer, reset_button = st.columns([9, 1])

with footer:
    st.markdown(
    """
    Design and Engineering Department,  
    Tower Bersama Group  
    Version 1.0
    """
)
with reset_button:
    if st.button(
        "Reset App",
        type="secondary",
        help="Click to reset the application state.",
        icon=":material/refresh:",
    ):
        reset_app()

# --------- END OF PAGE CONTENT --------- #