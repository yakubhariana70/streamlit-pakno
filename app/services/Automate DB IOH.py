import streamlit as st
import pandas as pd
import time
import os
from io import BytesIO
from datetime import date
from modules.db_update import (
    automate_db_update,
    load_dataframes,
)
from modules.dropsite import dropsite_processing, load_dropsite_data
from modules.dummy_database import (
    process_dummy_database,
    load_dummy_data,
)
from modules.utils import (
    find_best_match,
    sanitize_header,
    detect_version,
    detect_week,
)

# SECRETS
FILES_LOC = st.secrets["files_loc"]

# ----------  CACHED HELPERS  ---------- #
@st.cache_data(persist='disk', show_spinner=False)
def load_excel_bytes(content: bytes) -> dict[str, pd.DataFrame]:
    return pd.read_excel(BytesIO(content), sheet_name=None)


@st.cache_data(persist='disk', show_spinner=False)
def load_db_update(db_bytes: bytes, wo_bytes: bytes):
    return load_dataframes(BytesIO(db_bytes), BytesIO(wo_bytes))


@st.cache_data(persist='disk', show_spinner=False)
def load_dropsite(db_bytes: bytes, ds_bytes: bytes):
    return load_dropsite_data(BytesIO(db_bytes), BytesIO(ds_bytes))


@st.cache_data(persist='disk', show_spinner=False)
def load_dummy(db_bytes: bytes, ring_bytes: bytes):
    return load_dummy_data(BytesIO(db_bytes), BytesIO(ring_bytes))


# --------------  END OF CACHED HELPERS  ---------- #

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
# ------------- START OF APP ------------- #
# ---------------------------------------- #

st.set_page_config(
    page_title="Database Update Automation",
    page_icon=":material/home_storage:",
    layout="wide",
)

# DATABASE UPDATE TAB
st.title("DB IOH Automation")
st.markdown(
    """
    This application automates the process of updating the database with new ring data from work orders,  
    filtering out drop site data and generating a dummy database.
    """
)

# TABS
tabs = ["**Database Update**", "**Drop Site**", "**Dummy Database**"]
db_update, drop_site, dummy_db = st.tabs(tabs)

# Database Update Tab
with db_update:
    col_db, col_wo = st.columns(2)

    with st.form(key="db_update_form", clear_on_submit=True, border=False):
        # File upload
        with col_db:
            with st.container(height=200, border=False):
                st.subheader("Existing Database")
                st.markdown(
                    """
                    Upload the existing database file that contains the current ring data.  
                    This file will be updated with new ring data from the work order.
                    """
                )
                template_database = f"{FILES_LOC}/templates/Template - Database Update.xlsx"
                # if os.path.exists(template_database):
                with open(template_database, "rb") as file:
                    st.download_button(
                        type="tertiary",
                        key="dbupdate_db_template",
                        label="Download Database Template",
                        data=file,
                        file_name=os.path.basename(template_database),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        icon=":material/download:",
                        help="Click to download the template for the masterlist database.",
                    )

            db_exist = st.file_uploader(
                "Upload Existing Database File", type=["xlsx"], key="update_db"
            )
            used_sheets = ["Site List", "Length", "New Ring", "Insert Ring"]

            if db_exist:
                try:
                    db_content = db_exist.getvalue()
                    db_df = load_excel_bytes(db_content)
                    st.session_state["df_db_update"] = db_df
                    st.success(
                        f"✅ Database file '{os.path.basename(db_exist.name)}' loaded successfully."
                    )
                    db_filename = os.path.basename(db_exist.name)

                    st.write("#### **Existing Database Preview**")
                    for sheet_name, df in db_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading database file: {e}")
                    db_exist = None
        with col_wo:
            with st.container(height=200, border=False):
                st.subheader("Work Order")
                st.markdown(
                    """
                    Upload the work order file that contains new ring data,    
                    This file will be processed to update the existing database.
                    """
                )
                template_work_order = f"{FILES_LOC}/templates/Template - Work Order.xlsx"
                if os.path.exists(template_work_order):
                    with open(template_work_order, "rb") as file:
                        st.download_button(
                            type="tertiary",
                            key="update_wo_template",
                            label="Download Work Order Template",
                            data=file,
                            file_name=os.path.basename(template_work_order),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            icon=":material/download:",
                            help="Click to download the template for the work order.",
                        )

            work_order = st.file_uploader(
                "Upload work Order File", type=["xlsx"], key="update_wo"
            )
            if work_order:
                try:
                    work_order_content = work_order.getvalue()
                    work_order_df = load_excel_bytes(work_order_content)
                    st.session_state["work_order_df"] = work_order_df
                    st.success(
                        f"✅ Work order file '{os.path.basename(work_order.name)}' loaded successfully."
                    )
                    work_order_filename = os.path.basename(work_order.name)

                    st.write("#### **Work Order Preview**")
                    for sheet_name, df in work_order_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading work order file: {e}")
                    work_order = None

        # Initialize new_database in session state
        if "new_database" not in st.session_state:
            st.session_state["new_database"] = None

        # Action Button
        st.markdown("---")
        if st.form_submit_button(
            "Update Database",
            type="primary",
            help="Click to update the database with new ring data from the work order file.",
            disabled=not (db_exist and work_order),
            icon=":material/refresh:" + " " * 2,
        ):
            if not db_exist:
                st.error("Please upload an existing database file.")
            elif not work_order:
                st.error("Please upload a work order file.")
            if db_content and work_order_content:
                try:
                    with st.spinner("Updating database..."):
                        # Call the automation function
                        initial_data = load_db_update(
                            db_bytes=db_content, wo_bytes=work_order_content
                        )
                        version = detect_version(db_filename)
                        export_dir = f"{FILES_LOC}/exports/DB_Automation/Database_Update/{date.today().strftime('%Y-%m-%d')}"

                        if not os.path.exists(export_dir):
                            os.makedirs(export_dir)

                        # Filename
                        date_today = date.today().strftime("%Y%m%d")
                        week = detect_week(date_today)
                        export_filename = (
                            f"DB Update-{date_today}-Week {week}-TBG-{version}.xlsx"
                        )

                        new_database = automate_db_update(
                            initial_data,
                            new_database=export_filename,
                            export_dir=export_dir,
                            version=version,
                        )
                        st.session_state["new_database"] = new_database
                    st.success("Database update completed successfully!")
                except Exception as e:
                    st.error(f"Error during automation: {e}")

    # Download Result
    new_database = st.session_state.get("new_database")
    if new_database:
        st.markdown("---")
        st.markdown("#### **Download Updated Database**")
        st.write(
            """The updated database is ready for download.  
            Click the button below to download the new database file."""
        )
        st.write(f"New Database Available: {new_database}")
        with open(new_database, "rb") as file:
            st.download_button(
                type="primary",
                key="update_download",
                label="Download Result",
                data=file,
                file_name=os.path.basename(new_database),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                icon=":material/download:",
                help="Click to download the updated database file.",
            )

# Drop Site Tab
with drop_site:
    col_db, col_ds = st.columns(2)

    with st.form(key="drop_site_form", clear_on_submit=True, border=False):
        # File upload
        with col_db:
            with st.container(height=200, border=False):
                st.subheader("Masterlist Database")
                st.markdown(
                    """Upload the masterlist database file that contains the current ring data.     
                    This file will be filtered with drop site data."""
                )
                template_database = f"{FILES_LOC}/templates/Template - Database Update.xlsx"
                if os.path.exists(template_database):
                    with open(template_database, "rb") as file:
                        st.download_button(
                            type="tertiary",
                            key="ds_db_template",
                            label="Download Database Template",
                            data=file,
                            file_name=os.path.basename(template_database),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            icon=":material/download:",
                            help="Click to download the template for the masterlist database.",
                        )

            db_masterlist = st.file_uploader(
                "Upload Database File", type=["xlsx"], key="ds_db"
            )
            used_sheets = ["Site List", "Length", "New Ring", "Insert Ring", "Sheet1"]

            if db_masterlist:
                try:
                    db_content = db_masterlist.getvalue()
                    db_df = load_excel_bytes(db_content)
                    st.session_state["df_db_ds"] = db_df
                    st.success(
                        f"✅ Database file '{os.path.basename(db_masterlist.name)}' loaded successfully."
                    )
                    db_filename = os.path.basename(db_masterlist.name)

                    st.write("#### **Database Preview**")
                    for sheet_name, df in db_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading database file: {e}")
                    db_masterlist = None
        with col_ds:
            with st.container(height=200, border=False):
                st.subheader("Drop Site")
                st.markdown(
                    """Drop site files to filter out masterlist database.    
                    This file contains the drop site data that will be processed to update the masterlist database.
                    """
                )
                template_drop_site = f"{FILES_LOC}/templates/Template - Site Drop.xlsx"
                if os.path.exists(template_drop_site):
                    with open(template_drop_site, "rb") as file:
                        st.download_button(
                            type="tertiary",
                            key="ds_template",
                            label="Download Drop Site Template",
                            data=file,
                            file_name=os.path.basename(template_drop_site),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            icon=":material/download:",
                            help="Click to download the template for the drop site.",
                        )

            ds_file = st.file_uploader("Upload Drop Site File", type=["xlsx"], key="ds_wo")
            if ds_file:
                try:
                    ds_df_content = ds_file.getvalue()
                    ds_df = load_excel_bytes(ds_df_content)
                    st.session_state["df_ds"] = ds_df
                    st.success(
                        f"✅ Drop site file '{os.path.basename(ds_file.name)}' loaded successfully."
                    )
                    ds_file_filename = os.path.basename(ds_file.name)

                    st.write("#### **Drop site Preview**")
                    for sheet_name, df in ds_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading drop site file: {e}")
                    ds_file = None

        # Initialize new_database in session state
        if "dropped_site_database" not in st.session_state:
            st.session_state["dropped_site_database"] = None

        # Action Button
        st.markdown("---")
        if st.form_submit_button(
            "Process Drop Site",
            type="primary",
            help="Click to running the drop site automation.",
            disabled=not (db_masterlist and ds_file),
            icon=":material/refresh:" + " " * 2,
        ):
            if not db_masterlist:
                st.error("Please upload an existing database file.")
            elif not ds_file:
                st.error("Please upload a drop site file.")
            if db_content and ds_df_content:
                try:
                    with st.spinner("Processing drop site..."):
                        # Call the automation function
                        initial_data = load_dropsite(
                            db_bytes=db_content, ds_bytes=ds_df_content
                        )
                        export_dir = f"{FILES_LOC}/exports/DB_Automation/Drop_Site/{date.today().strftime('%Y-%m-%d')}"

                        if not os.path.exists(export_dir):
                            os.makedirs(export_dir)

                        # Filename
                        date_today = date.today().strftime("%Y%m%d")
                        week = detect_week(date_today)
                        version = detect_version(db_filename)
                        ds_file_filename = (
                            f"DB Dropped Site-{date_today}-Week {week}-TBG-{version}.xlsx"
                        )

                        dropped_site_database = dropsite_processing(
                            initial_data,
                            dropsite_filename=ds_file_filename,
                            export_dir=export_dir,
                        )
                        st.session_state["dropped_site_database"] = dropped_site_database
                    st.success("Database update completed successfully!")
                except Exception as e:
                    st.error(f"Error during automation: {e}")

    # Download Result
    dropped_site_database = st.session_state.get("dropped_site_database")
    if dropped_site_database:
        st.markdown("---")
        st.markdown("#### **Download Updated Database**")
        st.write(
            """
            The updated database is ready for download.  
            Click the button below to download the new database file.
            """
        )
        st.write(f"New Database Available: {dropped_site_database}")
        with open(dropped_site_database, "rb") as file:
            st.download_button(
                type="primary",
                key="ds_download",
                label="Download Result",
                data=file,
                file_name=os.path.basename(dropped_site_database),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                icon=":material/download:",
                help="Click to download the updated database file.",
            )

# Dummy Database Tab
with dummy_db:
    col_db, col_ring = st.columns(2)

    with st.form(key="dummy_db_form", clear_on_submit=True, border=False):
        with col_db:
            with st.container(height=200, border=False):
                st.subheader("Masterlist Database")
                st.markdown(
                    """Masterlist database file that contains the current ring data.    
                    This file will be updated with new ring data from the work order."""
                )
                template_database = f"{FILES_LOC}/templates/Template - Database Update.xlsx"
                if os.path.exists(template_database):
                    with open(template_database, "rb") as file:
                        st.download_button(
                            type="tertiary",
                            key="dummy_db_template",
                            label="Download Database Template",
                            data=file,
                            file_name=os.path.basename(template_database),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            icon=":material/download:",
                            help="Click to download the template for the masterlist database.",
                        )

            db_masterlist = st.file_uploader(
                "Upload Database File", type=["xlsx"], key="dummy_db"
            )
            used_sheets = ["Site List", "Length", "New Ring", "Insert Ring", "Sheet1"]

            if db_masterlist:
                try:
                    db_content = db_masterlist.getvalue()
                    db_df = load_excel_bytes(db_content)
                    st.session_state["df_db_dummy"] = db_df
                    st.success(
                        f"✅ Database file '{os.path.basename(db_masterlist.name)}' loaded successfully."
                    )
                    db_filename = os.path.basename(db_masterlist.name)

                    st.write("#### **Database Preview**")
                    for sheet_name, df in db_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading database file: {e}")
                    db_masterlist = None
        with col_ring:
            with st.container(height=200, border=False):
                st.subheader("Ring Data")
                st.markdown(
                    """Ring data files to generate a dummy database.    
                    This file will be processed to update the masterlist database."""
                )
                template_ring = f"{FILES_LOC}/templates/Template - Dummy Database.xlsx"
                if os.path.exists(template_ring):
                    with open(template_ring, "rb") as file:
                        st.download_button(
                            type="tertiary",
                            key="dummy_ring_template",
                            label="Download Insert Ring Template",
                            data=file,
                            file_name=os.path.basename(template_ring),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            icon=":material/download:",
                            help="Click to download the template for the ring data.",
                        )

            ring_file = st.file_uploader(
                "Upload Ring Data File", type=["xlsx"], key="dummy_wo"
            )
            if ring_file:
                try:
                    ring_file_content = ring_file.getvalue()
                    ring_file_df = load_excel_bytes(ring_file_content)
                    st.session_state["df_ring"] = ring_file_df
                    st.success(
                        f"✅ Ring data file '{os.path.basename(ring_file.name)}' loaded successfully."
                    )
                    ring_file_filename = os.path.basename(ring_file.name)

                    st.write("#### **Ring Data Preview**")
                    for sheet_name, df in ring_file_df.items():
                        bestmatch, score = find_best_match(sheet_name, used_sheets)
                        if bestmatch:
                            df = sanitize_header(df)
                            with st.expander(f"**{sheet_name}**"):
                                st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error loading ring data file: {e}")
                    ring_file = None

        # Initialize new_database in session state
        if "dummy_database" not in st.session_state:
            st.session_state["dummy_database"] = None

        # Action Button
        st.markdown("---")
        if st.form_submit_button(
            "Get Dummy Database",
            type="primary",
            help="Click to update the database with new ring data from the work order file.",
            disabled=not (db_masterlist and ring_file),
            icon=":material/refresh:" + " " * 2,
        ):
            if not db_masterlist:
                st.error("Please upload an masterlist database file.")
            elif not ring_file:
                st.error("Please upload a ring data file.")
            if db_masterlist and ring_file:
                try:
                    with st.spinner("Hold on, generating dummy database..."):
                        # Call the automation function
                        initial_data = load_dummy(
                            db_bytes=db_content, ring_bytes=ring_file_content
                        )
                        export_dir = f"{FILES_LOC}/exports/DB_Automation/Dummy_Database/{date.today().strftime('%Y-%m-%d')}"

                        if not os.path.exists(export_dir):
                            os.makedirs(export_dir)

                        # Filename
                        date_today = date.today().strftime("%Y%m%d")
                        week = detect_week(date_today)
                        version = detect_version(db_filename)
                        ring_file_filename = (
                            f"Dummy Database-{date_today}-Week {week}-TBG-{version}.xlsx"
                        )

                        dummy_database = process_dummy_database(
                            initial_data,
                            dummy_filename=ring_file_filename,
                            export_dir=export_dir,
                        )
                        st.session_state["dummy_database"] = dummy_database
                    st.success("Database update completed successfully!")
                except Exception as e:
                    st.error(f"Error during automation: {e}")

    # Download Result
    dummy_database = st.session_state.get("dummy_database")
    if dummy_database:
        st.markdown("---")
        st.markdown("#### **Download Updated Database**")
        st.write(
            """
            The updated database is ready for download.  
            Click the button below to download the new database file.
            """
        )
        st.write(f"New Database Available: {dummy_database['file_location']}")
        with open(dummy_database['file_location'], "rb") as file:
            st.download_button(
                type="primary",
                key="dummy_download",
                label="Download Result",
                data=file,
                file_name=os.path.basename(dummy_database['file_location']),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                icon=":material/download:",
                help="Click to download the updated database file.",
            )

# --- END OF TABS ---