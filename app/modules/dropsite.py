import pandas as pd
import os
from datetime import date
from modules.utils import (
    find_best_match, 
    sanitize_header, 
    detect_week, 
    stylize_sitelist, 
    stylize_length, 
    stylize_ring
    )

def load_dropsite_data(database: pd.ExcelFile, drop_site: pd.ExcelFile) -> dict:
    initial_data = {}
    try:
        with pd.ExcelFile(database) as db:
            try:
                sheet_names = db.sheet_names
                sheet_used = ['Site List', 'Length', 'New Ring']
                for sheet in sheet_used:
                    best_match, score = find_best_match(sheet, sheet_names)
                    if best_match:
                        print(f"Best match for '{sheet}': {best_match} | Score: {score:.2f}")
                        match sheet:
                            case 'Site List':
                                db_sitelist = pd.read_excel(db, sheet_name=best_match, na_filter=True)
                                db_sitelist = sanitize_header(db_sitelist)
                            case 'Length':
                                db_length = pd.read_excel(db, sheet_name=best_match)
                                db_length = sanitize_header(db_length)
                            case 'New Ring':
                                db_newring = pd.read_excel(db, sheet_name=best_match)
                                db_newring = sanitize_header(db_newring)
                        print(f"✅ {sheet} loaded successfully from '{best_match}'")
                        initial_data[sheet] = locals()[f"db_{sheet.lower().replace(' ', '')}"]
                    else:
                        print(f"No suitable match found for '{sheet}'")
                        raise ValueError(f"Sheet '{sheet}' not found in the database.")
                print("🔥📦 Database sheets loaded successfully. \n")
            except Exception as e:
                print(f"❌ Error loading sheets: {e}")
                raise

            sheet_not_used = [sheet for sheet in sheet_names if sheet not in sheet_used]
            if sheet_not_used:
                print(f"⚠️ Unused sheets in the database: {sheet_not_used}")
                nou_used = {}
                for sheet in sheet_not_used:
                    try:
                        df = pd.read_excel(db, sheet_name=sheet)
                        df = sanitize_header(df)
                        nou_used[sheet] = df
                        print(f"✅ Unused sheet '{sheet}' loaded successfully.")
                    except Exception as e:
                        print(f"❌ Error loading unused sheet '{sheet}': {e}")
                initial_data['Unused Sheets'] = nou_used

        with pd.ExcelFile(drop_site) as ds:
            try:
                sheet_names = ds.sheet_names
                drop_site = pd.read_excel(ds)
                drop_site = sanitize_header(drop_site)
                print(f"📍 Drop site data loaded successfully.")
                initial_data['Drop Site'] = drop_site
            except Exception as e:
                print(f"❌ Error loading drop site data: {e}")
                raise

        print("Processing drop site data...")
        print(f"Total drop sites: {len(drop_site)}")
        print(f"Total sites in database: {len(db_sitelist)}")
        print(f"Total rings in database: {len(db_newring)}")
        print(f"Total lengths in database: {len(db_length)}")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise
    return initial_data

def dropsite_processing(initial_data: dict, dropsite_filename: str, export_dir: str = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Export\Streamlit_Result\Drop_Site"):
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
        print(f"Export directory created: {export_dir}")
    dropsite_filename = os.path.join(export_dir, dropsite_filename)

    db_sitelist = initial_data['Site List']
    db_newring = initial_data['New Ring']
    db_length = initial_data['Length']

    drop_site = initial_data['Drop Site']
    date_today = str(date.today().strftime('%Y%m%d'))

    try:
        db_columns = db_sitelist.columns.tolist()
        db_newring_columns = db_newring.columns.tolist()
        dropsite_column = drop_site.columns.tolist()

        column_ds_site_id = find_best_match('Site ID', dropsite_column, threshold=0.7)[0]
        column_ds_ring_id = find_best_match('Ring ID', dropsite_column, threshold=0.7)[0]
        column_db_site_id = find_best_match('Site ID', db_columns, threshold=0.7)[0]
        column_db_ring_id = find_best_match('Ring ID', db_newring_columns, threshold=0.7)[0]
        

        if column_ds_site_id is None:
            raise ValueError("Site ID column not found in the drop site data.")
        if column_ds_ring_id is None:
            raise ValueError("Ring ID column not found in the drop site data.")

        dropsites_data = pd.DataFrame(columns=db_columns)
        not_found_sites = []

        for idx, row in drop_site.iterrows():
            site_id = row[column_ds_site_id]
            ring_id = row[column_ds_ring_id]

            if site_id in db_sitelist['Site ID'].values:
                print(f"✅ Site {site_id} found in the database. Dropping...")

                # Simpan data yang akan di-drop
                dropped = db_sitelist[db_sitelist['Site ID IOH'] == site_id].copy()
                if not dropped.empty:
                    print(f"📍 Dropped Site ID: {site_id} | Ring ID: {ring_id}")
                    dropsites_data = pd.concat([dropsites_data, dropped], ignore_index=True)
                    dropsites_data['date_dropped'] = date_today

                # Hapus dari site list
                db_sitelist = db_sitelist[db_sitelist['Site ID IOH'] != site_id].reset_index(drop=True)

                # Proses ring
                ring_exists = db_newring[db_newring[column_db_ring_id] == ring_id].copy()
                if ring_exists.empty:
                    print(f"❌ Ring ID {ring_id} not found in the New Ring data. Skipping drop.")
                    continue

                start_index = ring_exists.index[0]
                end_index = ring_exists.index[-1]
                top_part = db_newring.iloc[:start_index]
                bottom_part = db_newring.iloc[end_index + 1:]

                column_origin = find_best_match('Origin Site ID', db_newring_columns)[0]
                column_destination = find_best_match('Destination', db_newring_columns)[0]

                affected_rows = ring_exists[
                    (ring_exists[column_origin] == site_id) |
                    (ring_exists[column_destination] == site_id)
                ]
                before_site = affected_rows[affected_rows[column_destination] == site_id][column_origin].values
                after_site = affected_rows[affected_rows[column_origin] == site_id][column_destination].values

                new_ring = pd.DataFrame(columns=db_newring.columns)

                if before_site.size > 0 and after_site.size > 0:
                    origin = before_site[0]
                    destination = after_site[0]
                    print(f"🔄 Creating new connection: {origin} → {destination}")
                    row_data = {}

                    origin_data = ring_exists[ring_exists[column_origin] == origin]
                    destination_data = ring_exists[ring_exists[column_destination] == destination]

                    length = None
                    if not origin_data.empty and not destination_data.empty:
                        origin_distance = origin_data['Total Distance (m)'].values[0] if 'Total Distance (m)' in origin_data.columns else 0
                        destination_distance = destination_data['Total Distance (m)'].values[0] if 'Total Distance (m)' in destination_data.columns else 0
                        length = origin_distance + destination_distance

                    ref_row = ring_exists[
                        (ring_exists[column_origin] == origin) &
                        (ring_exists[column_destination] == site_id)
                    ]

                    if ref_row.empty:
                        ref_row = ring_exists[ring_exists[column_origin] == origin]

                    for col in db_newring.columns:
                        match col:
                            case 'Origin Site ID':
                                row_data[col] = origin
                            case 'Destination':
                                row_data[col] = destination
                            case 'Link Name':
                                row_data[col] = f"{origin}-{destination}"
                            case 'Total Distance (m)':
                                row_data[col] = float(length) if length is not None else 0
                            case _:
                                if col in ref_row.columns:
                                    row_data[col] = ref_row[col].values[0] if not ref_row[col].empty else None
                                else:
                                    best_match, score = find_best_match(col, db_newring_columns)
                                    if best_match and best_match in ref_row.columns:
                                        row_data[col] = ref_row[best_match].values[0]
                                    else:
                                        row_data[col] = None

                    new_ring.loc[0] = row_data
                    db_newring = pd.concat([top_part, new_ring, bottom_part], ignore_index=True)
                elif before_site.size > 0 and after_site.size == 0:
                    origin = before_site[0]
                    print(f"🔄 Creating new connection: {origin} → {site_id}")
                    row_data = {}

                    origin_data = ring_exists[ring_exists[column_origin] == origin]

                    length = None
                    if not origin_data.empty:
                        origin_distance = origin_data['Total Distance (m)'].values[0] if 'Total Distance (m)' in origin_data.columns else 0
                        length = origin_distance

                    ref_row = ring_exists[
                        (ring_exists[column_origin] == origin) &
                        (ring_exists[column_destination] == site_id)
                    ]
                    if ref_row.empty:
                        ref_row = ring_exists[ring_exists[column_origin] == origin]

                    for col in db_newring.columns:
                        match col:
                            case 'Origin Site ID':
                                row_data[col] = origin
                            case 'Destination':
                                row_data[col] = site_id
                            case 'Link Name':
                                row_data[col] = f"{origin}-{site_id}"
                            case 'Total Distance (m)':
                                row_data[col] = float(length) if length is not None else 0
                            case _:
                                if col in ref_row.columns:
                                    row_data[col] = ref_row[col].values[0] if not ref_row[col].empty else None
                                else:
                                    best_match, score = find_best_match(col, db_newring_columns)
                                    if best_match and best_match in ref_row.columns:
                                        row_data[col] = ref_row[best_match].values[0]
                                    else:
                                        row_data[col] = None

                    new_ring.loc[0] = row_data
                    db_newring = pd.concat([top_part, new_ring, bottom_part], ignore_index=True)
                elif before_site.size == 0 and after_site.size > 0:
                    destination = after_site[0]
                    print(f"🔄 Creating new connection: {site_id} → {destination}")
                    row_data = {}

                    destination_data = ring_exists[ring_exists[column_destination] == destination]

                    length = None
                    if not destination_data.empty:
                        destination_distance = destination_data['Total Distance (m)'].values[0] if 'Total Distance (m)' in destination_data.columns else 0
                        length = destination_distance

                    ref_row = ring_exists[
                        (ring_exists[column_origin] == site_id) &
                        (ring_exists[column_destination] == destination)
                    ]
                    if ref_row.empty:
                        ref_row = ring_exists[ring_exists[column_destination] == destination]

                    for col in db_newring.columns:
                        match col:
                            case 'Origin Site ID':
                                row_data[col] = site_id
                            case 'Destination':
                                row_data[col] = destination
                            case 'Link Name':
                                row_data[col] = f"{site_id}-{destination}"
                            case 'Total Distance (m)':
                                row_data[col] = float(length) if length is not None else 0
                            case _:
                                if col in ref_row.columns:
                                    row_data[col] = ref_row[col].values[0] if not ref_row[col].empty else None
                                else:
                                    best_match, score = find_best_match(col, db_newring_columns)
                                    if best_match and best_match in ref_row.columns:
                                        row_data[col] = ref_row[best_match].values[0]
                                    else:
                                        row_data[col] = None

                    new_ring.loc[0] = row_data
                    db_newring = pd.concat([top_part, new_ring, bottom_part], ignore_index=True)
                else:
                    print(f"❌ No valid connections found for Site ID {site_id} and Ring ID {ring_id}. Skipping drop.")
                    continue

                # Update ring length
                column_length_ring_id = find_best_match('Ring ID', db_length.columns)[0]
                new_ring_length = new_ring['Total Distance (m)'].sum()
                db_length.loc[db_length[column_length_ring_id] == ring_id, '#of Site'] = len(new_ring) - 1
                db_length.loc[db_length[column_length_ring_id] == ring_id, 'FO Distance (Meter)'] = new_ring_length
                db_length.loc[db_length[column_length_ring_id] == ring_id, 'AVG Length'] = new_ring_length / (len(new_ring) - 1) if len(new_ring) > 1 else 0
                db_length.loc[db_length[column_length_ring_id] == ring_id, 'date_updated'] = date_today

                print(f"✅ Site {site_id} and Ring ID {ring_id} dropped and updated.")
            else:
                print(f"❌ Site {site_id} not found in the database. Skipping drop.")
                not_found_sites.append(site_id)

        # Final export
        print("\nSummary of Drop Site")
        print(f"Total sites dropped: {len(dropsites_data)}")
        print(f"Total sites not found in the database: {len(not_found_sites)} | Sites: {not_found_sites}")
        print(f"Total sites remaining in the database: {len(db_sitelist):,}")
        print(f"Total lengths remaining in the database: {len(db_length):,}")
        print(f"Total rings remaining in the database: {len(db_newring):,}\n")

        # Stylize Dataframes
        db_sitelist = stylize_sitelist(db_sitelist)
        db_length = stylize_length(db_length)
        db_newring = stylize_ring(db_newring)
        
        print("Writing dropped site data to Excel...")
        with pd.ExcelWriter(dropsite_filename, engine='openpyxl', mode='w') as writer:
            db_sitelist.to_excel(writer, sheet_name='Site List', index=False)
            db_length.to_excel(writer, sheet_name='Length', index=False)
            db_newring.to_excel(writer, sheet_name='New Ring', index=False)

            if 'Unused Sheets' in initial_data:
                dropsite_sheet = 'Drop Site'
                find_best_match(dropsite_sheet, initial_data['Unused Sheets'].keys(), threshold=0.7)
                for sheet_name, df in initial_data['Unused Sheets'].items():
                    if sheet_name == dropsite_sheet:
                        joined_df = pd.concat([df, drop_site], ignore_index=True)
                        joined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                dropsites_data.to_excel(writer, sheet_name='Drop Site', index=False)
            print("✅ Dropped site data written successfully.")
        return dropsite_filename
    except Exception as e:
        print(f"❌ Error during drop site processing: {e}")
        raise

if __name__ == "__main__":
    database = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Export\20250702-Week 27-TBG-v2.xlsx"
    drop_site = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Data\site drop.xlsx"
    date_today = str(date.today().strftime('%Y%m%d'))
    dropsite_filename = f"{date_today}-Drop Site.xlsx"

    try:
        initial_data = load_dropsite_data(database, drop_site)
        dropsite_filename = dropsite_processing(initial_data, dropsite_filename)
        print(f"Drop site processing completed successfully: {dropsite_filename}")
    except Exception as e:
        print(f"❌ Error in main execution: {e}")