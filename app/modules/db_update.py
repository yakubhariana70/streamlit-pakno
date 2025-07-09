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

def load_dataframes(db_exist, work_order):
    print("Loading dataframes from the provided files...")
    print(f"Database file: {db_exist}")
    print(f"Work order file: {work_order}")

    # if not os.path.exists(db_exist):
    #     raise FileNotFoundError(f"Database file '{db_exist}' does not exist.")
    # if not os.path.exists(work_order):
    #     raise FileNotFoundError(f"Work order file '{work_order}' does not exist.")

    # PROCESSING INITIAL DATA
    initial_data = {
        'db_sitelist': None,
        'db_length': None,
        'db_newring': None,
        'db_notused': None,
        'wo_sitelist': None,
        'wo_newring': None,
        'wo_insertring': None,
        'wo_delsegment': None,
    }

    with pd.ExcelFile(db_exist) as db:
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
                            initial_data['db_sitelist'] = db_sitelist
                        case 'Length':
                            db_length = pd.read_excel(db, sheet_name=best_match)
                            db_length = sanitize_header(db_length)
                            initial_data['db_length'] = db_length
                        case 'New Ring':
                            db_newring = pd.read_excel(db, sheet_name=best_match)
                            db_newring = sanitize_header(db_newring)
                            initial_data['db_newring'] = db_newring
                    print(f"✅ {sheet} loaded successfully from '{best_match}'")

                else:
                    print(f"No suitable match found for '{sheet}'")
                    raise ValueError(f"Sheet '{sheet}' not found in the database.")
                
            sheet_not_used = [sheet for sheet in sheet_names if sheet not in sheet_used]
            if sheet_not_used:
                print(f"⚠️ The following sheets are not used: {sheet_not_used}")
                not_used = {}
                for sheet in sheet_not_used:
                    data = pd.read_excel(db, sheet_name=sheet)
                    data = sanitize_header(data)
                    not_used[sheet] = data
                initial_data['db_notused'] = not_used
            print("🔥📦 Database sheets loaded successfully. \n")
        except Exception as e:
            print(f"❌ Error loading sheets: {e}")
            raise

    with pd.ExcelFile(work_order) as wo:
        try:
            sheet_names = wo.sheet_names
            sheet_used = ['Site List','New Ring', 'Insert Ring', 'Del Segment']
            for sheet in sheet_used:
                best_match, score = find_best_match(sheet, sheet_names)
                if best_match:
                    print(f"Best match for '{sheet}': {best_match} | Score: {score:.2f}")
                    match sheet:
                        case 'Site List':
                            wo_sitelist = pd.read_excel(wo, sheet_name=best_match)
                            wo_sitelist = sanitize_header(wo_sitelist)
                            initial_data['wo_sitelist'] = wo_sitelist
                        case 'New Ring':
                            wo_newring = pd.read_excel(wo, sheet_name=best_match)
                            wo_newring = sanitize_header(wo_newring)
                            initial_data['wo_newring'] = wo_newring
                        case 'Insert Ring':
                            wo_insertring = pd.read_excel(wo, sheet_name=best_match)
                            wo_insertring = sanitize_header(wo_insertring)
                            initial_data['wo_insertring'] = wo_insertring
                        case 'Del Segment':
                            wo_delsegment = pd.read_excel(wo, sheet_name=best_match)
                            wo_delsegment = sanitize_header(wo_delsegment)
                            initial_data['wo_delsegment'] = wo_delsegment
                    print(f"✅ {sheet} loaded successfully from '{best_match}'")
                else:
                    print(f"No suitable match found for '{sheet}'")
                    raise ValueError(f"Sheet '{sheet}' not found in the Work order.")
                
            print("🔥📦 Work order sheets loaded successfully. \n")
        except Exception as e:
            print(f"❌ Error loading sheets: {e}")
            raise

    # Check if all required DataFrames are loaded
    for key, df in initial_data.items():
        if df is None:
            raise ValueError(f"DataFrame '{key}' is not loaded properly. Please check the input files.")
    print("All required DataFrames loaded successfully.")
    return initial_data
    

def automate_db_update(initial_data, new_database:str=None, version="v1", export_dir=r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Export\Streamlit_Result\DB_Update"):
    # DESTRUCTURING INITIAL DATA
    db_sitelist = initial_data['db_sitelist']
    db_length = initial_data['db_length']
    db_newring = initial_data['db_newring']
    wo_sitelist = initial_data['wo_sitelist']
    wo_newring = initial_data['wo_newring']
    wo_insertring = initial_data['wo_insertring']
    wo_delsegment = initial_data['wo_delsegment']

    # Check if all required DataFrames are loaded
    required_dfs = [db_sitelist, db_length, db_newring, wo_sitelist, wo_newring, wo_insertring, wo_delsegment]
    for df in required_dfs:
        if df is None or df.empty:
            raise ValueError("One or more required DataFrames are not loaded properly or are empty. Please check the input files.")
    print("All required DataFrames are loaded and valid.")

    # =========================
    # INITIALIZE NEW DATABASE
    # =========================
    date_today = date.today().strftime('%Y%m%d')
    week = detect_week(date_today)
    
    if new_database is None:
        print(f"Today's date: {date_today} | Week number: {week}")
        new_database = f"{date_today}-Week {week}-TBG-{version}.xlsx"
        
    new_database = f"{export_dir}/{new_database}" if export_dir else f"{os.path.dirname(db_exist)}/{new_database}"


    # Prepare target DataFrames for writing
    target_sitelist = db_sitelist.copy()
    target_length = db_length.copy()
    target_newring = db_newring.copy()

    # =========================
    # PROCESSING NEW SITE
    # =========================

    # New Site | Sitelist
    new_site = wo_newring[wo_newring['Existing/New Site_1'].astype(str).str.lower().str.replace(' ', '') == 'newsite'].reset_index(drop=True)
    if new_site.empty:
        print("❌ No new sites found in the Work order.")

    target_columns = target_sitelist.columns.tolist()
    source_columns = wo_newring.columns.tolist()

    container_newsite = pd.DataFrame(columns=target_columns)

    for idx, row in new_site.iterrows():
        site_id = row['Origin Site ID']
        sitelist_info = wo_sitelist[wo_sitelist['Site ID IOH'] == site_id]
        for col in target_columns:
            match col:
                case 'Site ID' | 'Site ID IOH':
                    container_newsite.loc[idx, col] = site_id
                    print(f"Processing Site ID: {site_id} | Column: {col}")
                case 'Site Name':
                    container_newsite.loc[idx, col] = row.get('Origin_Name', None)
                case 'Program Name':
                    container_newsite.loc[idx, col] = row.get('Program', None)
                case 'Program Ring':
                    container_newsite.loc[idx, col] = row.get('Program Ring', None)
                case 'Program Status':
                    container_newsite.loc[idx, col] = row.get('Existing/New Site_1', 'New Site')
                case 'insert/new ring':
                    container_newsite.loc[idx, col] = row.get('Ring Status', "new ring")
                case 'SoW' | 'Site Owner' | 'Initial Site ID' | 'Initial Site Name':
                    if not sitelist_info.empty:
                        container_newsite.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                    else:
                        print(f"❌ No sitelist info found for Site ID: {site_id} | Column: {col}")
                        container_newsite.loc[idx, col] = None
                case _:
                    if col in row:
                        container_newsite.loc[idx, col] = row[col]
                    elif col in sitelist_info.columns:
                        container_newsite.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                        print(f"Using sitelist info for column '{col}'")
                    else:
                        best_match, score = find_best_match(col, source_columns)
                        if best_match and best_match in row:
                            print(f"Using best match '{best_match}' for column '{col}'")
                            container_newsite.loc[idx, col] = row[best_match]
                        else:
                            print(f"❌ No match found for column '{col}' in new site data.")
                            container_newsite.loc[idx, col] = None

        # Check if the site already exists in the target sitelist
        if site_id in target_sitelist['Site ID'].values:
            print(f"⚠️ Site {site_id} already exists in the sitelist.")
        else:
            print(f"✅ New site {site_id} added to the sitelist.")

        container_newsite.loc[idx, 'date_updated'] = date_today

    # UPDATE SITE LIST
    target_sitelist = pd.concat([target_sitelist, container_newsite]).reset_index(drop=True)
    target_sitelist['No'] = range(1, len(target_sitelist) + 1)

    # New Site | Length
    ring_column = find_best_match('Ring ID', source_columns)[0]
    newring_list = wo_newring[ring_column].dropna().unique().tolist()
    if not newring_list:
        print("❌ No new rings found in the Work order.")

    target_columns = target_length.columns.tolist()
    source_columns = wo_newring.columns.tolist()

    newring_container_length = pd.DataFrame(columns=target_columns)

    for idx, ring in enumerate(newring_list):
        ring_data = wo_newring[wo_newring[ring_column] == ring]
        if ring_data.empty:
            print(f"❌ No data found for Ring ID: {ring}")
            continue

        fo_distance = ring_data['Total Distance (m)'].sum() if 'Total Distance (m)' in ring_data.columns else None

        for col in target_columns:
            match col:
                case 'Ring ID':
                    newring_container_length.loc[idx, col] = ring
                    print(f"Processing Ring ID: {ring} | Column: {col}")
                case '#of Site':
                    total_segments = len(ring_data) - 1
                    newring_container_length.loc[idx, col] = total_segments
                case 'FO Distance (Meter)':
                    if fo_distance is not None:
                        newring_container_length.loc[idx, col] = fo_distance
                    else:
                        print(f"❌ No distance data found for Ring ID: {ring} | Column: {col}")
                        newring_container_length.loc[idx, col] = None
                case 'Vendor':
                    vendor = ring_data['Vendor'].iloc[0] if 'Vendor' in ring_data.columns else None
                    newring_container_length.loc[idx, col] = vendor
                case 'AVG Length':
                    average_length = fo_distance / total_segments if total_segments > 0 else None
                    newring_container_length.loc[idx, col] = average_length
                case 'Ring Status':
                    ring_status = ring_data['Ring Status'].iloc[0] if 'Ring Status' in ring_data.columns else None
                    newring_container_length.loc[idx, col] = ring_status
                case _:
                    best_match, score = find_best_match(col, source_columns)
                    if best_match and best_match in ring_data.columns:
                        newring_container_length.loc[idx, col] = ring_data[best_match].iloc[0]
                        print(f"Using best match '{best_match}' for column '{col}'")
                    else:
                        # print(f"❌ No match found for column '{col}' in new ring data.")
                        newring_container_length.loc[idx, col] = None

    newring_container_length['No'] = range(1, len(newring_container_length) + 1)
    newring_container_length['date_updated'] = date_today

    if target_length.empty:
        target_length = newring_container_length
        print("✅ New ring length data added to the target length.")
    else:
        target_length = pd.concat([target_length, newring_container_length]).reset_index(drop=True)
        print("✅ Existing ring length data updated with new ring data.")

    # New Site | New Ring
    target_newring = pd.concat([target_newring, wo_newring]).reset_index(drop=True)

    # =========================
    # SUMMARY OF NEW SITE PROCESSING
    # =========================
    print(f"Total New Site Processed: {len(container_newsite)}")
    print(f"Total Length Processed: {len(newring_container_length)}")
    print(f"Total New Ring Processed: {len(wo_newring)}")

    # =========================
    # PROCESSING INSERT RING
    # =========================

    # Insert Ring | Sitelist
    ir_site = wo_insertring[(wo_insertring['Existing/New Site_1'].astype(str).str.lower().str.replace(' ', '') == 'newsite')
                            & (wo_insertring['Priority_1'].astype(str).str.lower().str.replace(' ', '') == 'insertsite')
                            ].reset_index(drop=True)
    if ir_site.empty:
        print("❌ No new insert rings sites found in the Work order.")

    target_columns = target_sitelist.columns.tolist()
    source_columns = wo_insertring.columns.tolist()

    container_ir_site = pd.DataFrame(columns=target_columns)

    for idx, row in ir_site.iterrows():
        site_id = row['Origin Site ID']
        sitelist_info = wo_sitelist[wo_sitelist['Site ID IOH'] == site_id]
        for col in target_columns:
            match col:
                case 'Site ID' | 'Site ID IOH':
                    container_ir_site.loc[idx, col] = site_id
                case 'Site Name':
                    container_ir_site.loc[idx, col] = row.get('Origin_Name', None)
                case 'Program Name':
                    container_ir_site.loc[idx, col] = row.get('Program', None)
                case 'Program Ring':
                    container_ir_site.loc[idx, col] = row.get('Program Ring', None)
                case 'Program Status':
                    container_ir_site.loc[idx, col] = row.get('Existing/New Site_1', 'New Site')
                case 'insert/new ring':
                    container_ir_site.loc[idx, col] = row.get('Ring Status', "new ring")
                case 'SoW' | 'Site Owner' | 'Initial Site ID' | 'Initial Site Name':
                    if not sitelist_info.empty:
                        container_ir_site.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                    else:
                        print(f"❌ No sitelist info found for Site ID: {site_id} | Column: {col}")
                        container_ir_site.loc[idx, col] = None
                case _:
                    if col in row:
                        container_ir_site.loc[idx, col] = row[col]
                    elif col in sitelist_info.columns:
                        container_ir_site.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                        print(f"Using sitelist info for column '{col}'")
                    else:
                        best_match, score = find_best_match(col, source_columns)
                        if best_match and best_match in row:
                            print(f"Using best match '{best_match}' for column '{col}'")
                            container_ir_site.loc[idx, col] = row[best_match]
                        else:
                            # print(f"❌ No match found for column '{col}' in new site data.")
                            container_ir_site.loc[idx, col] = None

        # Check if the site already exists in the target sitelist
        if site_id in target_sitelist['Site ID'].values:
            print(f"⚠️ Site {site_id} already exists in the sitelist.")
        else:
            print(f"✅ New site {site_id} added to the sitelist.")

        container_ir_site.loc[idx, 'date_updated'] = date_today

    print("Summary update Insert Ring Site Data")
    print(f"Total Insert Ring Site Before Update: {len(db_sitelist)}")
    print(f"Total Insert Ring Sites to be updated: {len(container_ir_site)}")

    # Update sitelist
    target_sitelist = pd.concat([target_sitelist, container_ir_site]).reset_index(drop=True)

    # Insert Ring | Length
    ring_column = find_best_match('Ring ID', source_columns)[0]
    insertring_list = wo_insertring[ring_column].dropna().unique().tolist()
    if not insertring_list:
        print("❌ No new rings found in the Work order.")

    target_columns = target_length.columns.tolist()
    source_columns = wo_insertring.columns.tolist()

    # container_length = pd.DataFrame(columns=target_columns)
    ir_container_length = pd.DataFrame(columns=target_columns)
    for idx, ring in enumerate(insertring_list):
        ring_data = wo_insertring[wo_insertring[ring_column] == ring]
        if ring_data.empty:
            print(f"❌ No data found for Ring ID: {ring}")
            continue
        fo_distance = ring_data['Total Distance (m)'].sum() if 'Total Distance (m)' in ring_data.columns else None

        for col in target_columns:
            match col:
                case 'Ring ID':
                    ir_container_length.loc[idx, col] = ring
                    print(f"Processing Ring ID: {ring} | Column: {col}")
                case '#of Site':
                    total_segments = len(ring_data) - 1
                    ir_container_length.loc[idx, col] = total_segments
                case 'FO Distance (Meter)':
                    if fo_distance is not None:
                        ir_container_length.loc[idx, col] = fo_distance
                    else:
                        print(f"❌ No distance data found for Ring ID: {ring} | Column: {col}")
                        ir_container_length.loc[idx, col] = None
                case 'Vendor':
                    vendor = ring_data['Vendor'].iloc[0] if 'Vendor' in ring_data.columns else None
                    ir_container_length.loc[idx, col] = vendor
                case 'AVG Length':
                    average_length = fo_distance / total_segments if total_segments > 0 else None
                    ir_container_length.loc[idx, col] = average_length
                case 'Ring Status':
                    ring_status = ring_data['Ring Status'].iloc[0] if 'Ring Status' in ring_data.columns else None
                    ir_container_length.loc[idx, col] = ring_status
                case _:
                    best_match, score = find_best_match(col, source_columns)
                    if best_match and best_match in ring_data.columns:
                        ir_container_length.loc[idx, col] = ring_data[best_match].iloc[0]
                        print(f"Using best match '{best_match}' for column '{col}'")
                    else:
                        # print(f"❌ No match found for column '{col}' in new ring data.")
                        ir_container_length.loc[idx, col] = None

    ir_container_length['No'] = ir_container_length.index + 1

    # UPDATE TARGET LENGTH
    for idx, row in ir_container_length.iterrows():
        ring_id = row['Ring ID']
        target = target_length[target_length['Ring ID'] == ring_id]
        if target.empty:
            print(f"❌ Ring ID: {ring_id} not found in target length. Skipping update.")
            continue
        elif len(target) > 1:
            print(f"⚠️ Multiple entries found for Ring ID: {ring_id}. Updating the first entry only.")
            raise ValueError(f"Multiple entries found for Ring ID: {ring_id}. Please check the data.")
        
        target = target.iloc[0]
        if ring_id in target_length['Ring ID'].values:
            for col in target_columns:
                if col == 'Ring Status':
                    continue
                elif col in row:
                    target_length.loc[target.name, col] = row[col]
            target_length.loc[target.name, 'date_updated'] = f"{date_today}"
        else:
            print(f"⚠️ Ring ID: {ring_id} not found in target length. Skipping update.")

    # New Ring | New Ring
    target_columns = target_newring.columns.tolist()
    source_columns = wo_insertring.columns.tolist()

    for num, ring_id in enumerate(insertring_list):
        source_data = wo_insertring[wo_insertring[ring_column] == ring_id]
        if source_data.empty:
            print(f"❌ No data found for Ring ID: {ring_id}. Skipping update.")
            continue
        
        target = target_newring[target_newring['Ring ID_1'] == ring_id]
        if target.empty:
            print(f"❌ Ring ID: {ring_id} not found in target new ring. Skipping update.")
            continue

        total_exist = len(target)
        total_update = len(source_data)

        if total_exist == 0:
            print(f"⚠️ No existing entries found for Ring ID: {ring_id}.")

        start_index = target.index[0] if not target.empty else None
        end_index = target.index[-1] if not target.empty else None

        column_origin = find_best_match('Origin Site ID', target_columns)[0]
        column_destination = find_best_match('Destination', target_columns)[0]
        column_origin_priority = find_best_match('Priority_1', source_columns)[0]
        column_destination_priority = find_best_match('Priority_2', source_columns)[0]
        column_link = find_best_match('Link Name', source_columns)[0]

        origin_insert = source_data[source_data[column_origin_priority].astype(str).str.lower().str.replace(' ', '') == 'insertsite']
        destination_insert = source_data[source_data[column_destination_priority].astype(str).str.lower().str.replace(' ', '') == 'insertsite']

        link_origin = origin_insert[column_link].dropna().unique().tolist()
        link_destination = destination_insert[column_link].dropna().unique().tolist()
        origin_site_ids = origin_insert[column_origin].dropna().unique().tolist()
        destination_site_ids = destination_insert[column_destination].dropna().unique().tolist()
        print(f"Origin Site IDs: {origin_site_ids} | Destination Site IDs: {destination_site_ids}")

        origin_connections = [link.split("-")[1] for link in link_origin]
        destination_connections = [link.split("-")[0] for link in link_destination]
        
        existing_origin = target[column_origin].dropna().unique().tolist()
        existing_destination = target[column_destination].dropna().unique().tolist()

        top_part = target_newring.iloc[:start_index]
        bottom_part = target_newring.iloc[end_index + 1:] if end_index is not None else pd.DataFrame()

        print(f"Processing Ring ID: {ring_id} | Start index: {start_index} | End index: {end_index}")
        print(f"Total existing entries: {total_exist} | Total new entries: {total_update}")

        new_data = pd.DataFrame(columns=target_columns)

        for idx, row in source_data.iterrows():
            origin_in_existing = row[column_origin] in existing_origin
            destination_in_existing = row[column_destination] in existing_destination
            origin_in_new = row[column_origin] in origin_site_ids and row[column_origin] not in existing_origin
            destination_in_new = row[column_destination] in destination_site_ids and row[column_destination] not in existing_destination

            if origin_in_new and not destination_in_new:
                print(f"✅ New Origin connection: Ring ID: {ring_id} | Origin: {row[column_origin]}")
            elif destination_in_new and not origin_in_new:
                print(f"✅ New Destination connection: Ring ID: {ring_id} | Destination: {row[column_destination]}")
            elif origin_in_new and destination_in_new:
                print(f"✅ New Connection: Ring ID: {ring_id} | Origin: {row[column_origin]} | Destination: {row[column_destination]}")
            elif origin_in_existing and not destination_in_existing:
                print(f"🔃 Existing Origin: Ring ID: {ring_id} | Origin: {row[column_origin]} | Destination: {row[column_destination]}")
            elif not origin_in_existing and destination_in_existing:
                print(f"🔃 Existing Destination: Ring ID: {ring_id} | Origin: {row[column_origin]} | Destination: {row[column_destination]}")
            elif origin_in_existing and destination_in_existing:
                print(f"🔃 Existing Connection: Ring ID: {ring_id} | Origin: {row[column_origin]} | Destination: {row[column_destination]}")
            else:
                print(f"⚠️ No valid connection found for Ring ID: {ring_id} | Origin: {row[column_origin]} | Destination: {row[column_destination]}")
                continue
            
            for col in target_columns:
                if col in row:
                    new_data.loc[idx, col] = row[col]
                else:
                    best_match, score = find_best_match(col, source_columns)
                    if best_match and best_match in row:
                        new_data.loc[idx, col] = row[best_match]
                        # print(f"Using best match '{best_match}' for column '{col}'")
                    else:
                        # print(f"⚠️ No match found for column '{col}' in new ring data.")
                        new_data.loc[idx, col] = None
        
        new_data['Ring ID_1'] = ring_id
        new_data['date_updated'] = date_today

        if top_part.empty and bottom_part.empty:
            target_newring = pd.concat([target_newring, new_data]).reset_index(drop=True)
            print(f"✅ Added new data for Ring ID: {ring_id} at the end of the target new ring.\n")
        else:
            target_newring = pd.concat([top_part, new_data, bottom_part]).reset_index(drop=True)
            print(f"✅ Inserted new data for Ring ID: {ring_id} between existing entries.\n")

    # =========================
    # SUMMARY OF INSERT RING PROCESSING
    # =========================
    print(f"Total Insert Ring Site Processed: {len(container_ir_site)}")
    print(f"Total Length Processed: {len(ir_container_length)}")
    print(f"Total Insert Ring Processed: {len(wo_insertring)}")
    # =========================

    # =========================
    # FINALIZE PROCESSING
    # =========================
    target_sitelist['No'] = range(1, len(target_sitelist) + 1)
    target_length['No'] = range(1, len(target_length) + 1)
    target_newring['No'] = range(1, len(target_newring) + 1)

    summary_db_update = pd.DataFrame({
        'Date': [date_today],
        'Week': [week],
        'Version': [version],
        'Total Site List Updated': [len(target_sitelist)],
        'Total Length Updated': [len(target_length)],
        'Total New Ring Updated': [len(target_newring)]
    }).transpose()
    
    print("\nSummary of Database Update:\n")
    print(summary_db_update)

    # Stylize Dataframes
    target_sitelist = stylize_sitelist(target_sitelist)
    target_length = stylize_length(target_length)
    target_newring = stylize_ring(target_newring)
    
    with pd.ExcelWriter(new_database, engine='openpyxl', mode='w') as writer:
        target_sitelist.to_excel(writer, sheet_name='Site List', index=False)
        target_length.to_excel(writer, sheet_name='Length', index=False)
        target_newring.to_excel(writer, sheet_name='New Ring', index=False)
        summary_db_update.to_excel(writer, sheet_name='Summary', index=True, header=False)
        
        if 'db_notused' in initial_data and initial_data['db_notused']:
            not_used = initial_data['db_notused']
            for sheet_name, df in not_used.items():
                if sheet_name in writer.sheets:
                    print(f"⚠️ Sheet '{sheet_name}' already exists in the new database. Skipping.")
                else:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"ℹ️ Sheet '{sheet_name}' added to the new database.")
                
        print(f"✅ New database created: {new_database}")
    print("👍🔥 Insert Ring Data updated successfully.")
    return new_database

if __name__ == "__main__":
    # ==================
    # DATA IN PROCESSING 
    # ==================
    db_exist = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Data\202506018-Week25-TBG-v1_before.xlsx"
    work_order = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Data\20250603-H2B1NewSite-TBG-v9.xlsx"

    print("Starting the database update process...")

    try:
        initial_data = load_dataframes(db_exist, work_order)
        result_path = automate_db_update(initial_data)
    except Exception as e:
        print(f"❌ An error occurred during the database update: {e}")
    else:
        print("✅ Database update completed successfully.")