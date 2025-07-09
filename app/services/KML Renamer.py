import os
from datetime import date
import streamlit as st
import pandas as pd
import time
from io import BytesIO
from modules.utils import sanitize_header
from modules.rename_att_kml import rename_kml_field


# SECRETS
FILES_LOC = st.secrets["files_loc"]

# ----------  CACHED HELPERS  ---------- #
@st.cache_data(persist='disk', show_spinner=False)
def load_kml_bytes(content: bytes) -> str:
    if isinstance(content, bytes):
        return content.decode('utf-8')
    else:
        raise ValueError("Content must be in bytes format.")

@st.cache_data(persist='disk', show_spinner=False)
def load_excel_bytes(content: bytes) -> dict[str, pd.DataFrame]:
    return pd.read_excel(BytesIO(content), sheet_name=None, engine='openpyxl')


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
    page_title="KML Renamer",
    page_icon=":material/file_upload:",
    layout="wide",
)

# DATABASE UPDATE TAB
st.title("KML Renamer Automation")
st.markdown(
    """
    KML Renamer is a tool designed to automate the renaming of KML files based on specific criteria.
    """
)
st.markdown("---")

# Database Update Tab
col_kml, col_mapping = st.columns(2)

with st.form(key="kml_renamer_form", clear_on_submit=True, border=False):
    # File upload
    with col_kml:
        with st.container(height=150, border=False):
            st.subheader("KML File")
            st.markdown(
                """
                Upload KML file that contains the data to be renamed.   
                This file will be processed to rename fields based on the mapping   
                provided in the attribute file.
                """
            )

        kml_file = st.file_uploader(
            "Upload KML File", type=["kml"], key="kml_file", help="Upload a KML file that contains the data to be renamed."
        )
        if kml_file:
            try:
                kml_content = kml_file.getvalue()
                kml_content = load_kml_bytes(kml_content)
                st.session_state["kml_content"] = kml_content
                kml_filename = os.path.basename(kml_file.name)
                st.success(
                    f"✅ Database file '{kml_filename}' loaded successfully."
                )
            except Exception as e:
                st.error(f"Error loading database file: {e}")
                db_exist = None
            
            st.write("#### **Field to Rename**")
            st.markdown("""
            Specify the field in the KML file that you want to rename.
            The field name should match the column name in the KML file.
            """)
            checked_field = st.text_input(
                "Field to Rename",
                value="site id",
                key="checked_field",
                help="Enter the field name in the KML file that you want to rename.",
            )
            if not checked_field:
                st.warning("Please enter a field name to rename in the KML file.")

    with col_mapping:
        with st.container(height=150, border=False):
            st.subheader("Attribute Mapping")
            st.markdown(
                """
                Excel file containing the mapping of field names to change,     
                file should contain only 1 sheet + before and after column.
                """
            )

        map_file = st.file_uploader(
            "Mapping File",
            type=["xlsx", "xls"],
            key="map_file",
            help="Upload an Excel file containing the mapping of field names to change.",
        )
        if map_file:
            try:
                mapfile_content = map_file.getvalue()
                mapfile_df = load_excel_bytes(mapfile_content)
                mapfile_filename = os.path.basename(map_file.name)
                st.session_state["mapfile_df"] = mapfile_df
                st.success(
                    f"✅ Mapping file '{mapfile_filename}' loaded successfully."
                )

                st.write("#### **Mapping File Preview**")
                for sheet_name, df in mapfile_df.items():
                    df = sanitize_header(df)
                    with st.expander(f"**{sheet_name}**"):
                        st.dataframe(df.head())
            except Exception as e:
                st.error(f"Error loading work order file: {e}")
                work_order = None

    # Initialize new_database in session state
    if "kml_renamed" not in st.session_state:
        st.session_state["kml_renamed"] = None

    # Action Button
    st.markdown("---")
    if st.form_submit_button(
        "Process KML",
        type="primary",
        help="Click to process the KML file with the provided mapping.",
        disabled=not (kml_file and map_file),
        icon=":material/refresh:" + " " * 2,
    ):
        if kml_file and map_file:
            try:
                mapfile_df = st.session_state["mapfile_df"]
                kml_content = st.session_state["kml_content"]

                if not kml_content:
                    st.error("KML content is empty or not loaded.")
                if not mapfile_df:
                    st.error("Mapping file is empty or not loaded.")

                export_dir = f"{FILES_LOC}/exports/KML_Renamer/{date.today().strftime('%Y-%m-%d')}"
                if not os.path.exists(export_dir):
                    os.makedirs(export_dir)

                path = os.path.join(export_dir, kml_filename.replace(".kml", "_revised.kml"))
                revised_kml = rename_kml_field(
                    kml_content=kml_content,
                    attribute_df=mapfile_df[list(mapfile_df.keys())[0]],
                    checked_field=checked_field,
                    export_path=path,
                )
                st.session_state["kml_renamed"] = revised_kml
                st.success(
                    f"✅ KML file processed successfully. Renamed file saved as: {os.path.basename(path)}"
                )

            except Exception as e:
                st.error(f"Error processing KML file: {e}")
        else:
            st.warning("Please upload both KML and mapping files.")
        

# Download Result
kml_renamed = st.session_state.get("kml_renamed", None)
if kml_renamed:
    st.markdown("---")
    st.markdown("#### **Download Renamed KML**")
    st.write(
        """The updated database is ready for download.  
        Click the button below to download the renamed KML file."""
    )
    st.write(f"New KML file: {kml_filename.replace('.kml', '_renamed.kml')}")
    with open(kml_renamed, "rb") as file:
        st.download_button(
            type="primary",
            key="update_download",
            label="Download Result",
            data=file,
            file_name=os.path.basename(kml_renamed),
            mime="application/vnd.google-earth.kml+xml",
            icon=":material/download:",
            help="Click to download the renamed KML file.",
            disabled=not kml_renamed,
        )
# ---------------------------------------- #
# ------------- END OF APP --------------- #
# ---------------------------------------- #