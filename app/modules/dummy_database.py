import pandas as pd
import os
from datetime import date
from modules.utils import find_best_match, sanitize_header, detect_week, stylize_ring, stylize_length, stylize_sitelist

def load_dummy_data(database:pd.ExcelFile, ringlist:pd.ExcelFile) -> dict:
    initial_data = {}
    try:
        with pd.ExcelFile(database) as db:
            try:
                sheet_names = db.sheet_names
                print(f"Available sheets in database: {sheet_names}")
                sheet_used = ['Site List', 'Length', 'New Ring']
                
                for sheet in sheet_used:
                    best_match, score = find_best_match(sheet, sheet_names)
                    if best_match and score > 0:
                        print(f"Best match for '{sheet}': {best_match} | Score: {score:.2f}")
                        try:
                            match sheet:
                                case 'Site List':
                                    db_sitelist = pd.read_excel(db, sheet_name=best_match, na_filter=True)
                                    db_sitelist = sanitize_header(db_sitelist)
                                    initial_data['db_sitelist'] = db_sitelist
                                    print(f"✅ Site List loaded: {len(db_sitelist)} rows")
                                case 'Length':
                                    db_length = pd.read_excel(db, sheet_name=best_match)
                                    db_length = sanitize_header(db_length)
                                    initial_data['db_length'] = db_length
                                    print(f"✅ Length loaded: {len(db_length)} rows")
                                case 'New Ring':
                                    db_newring = pd.read_excel(db, sheet_name=best_match)
                                    db_newring = sanitize_header(db_newring)
                                    initial_data['db_newring'] = db_newring
                                    print(f"✅ New Ring loaded: {len(db_newring)} rows")
                        except Exception as sheet_error:
                            print(f"❌ Error loading sheet '{best_match}': {sheet_error}")
                            raise
                    else:
                        print(f"No suitable match found for '{sheet}' in sheets: {sheet_names}")
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
                else:
                    print("ℹ️ No unused sheets found in the database.")

                # Validate all required sheets were loaded
                required_keys = ['db_sitelist', 'db_length', 'db_newring']
                missing_keys = [key for key in required_keys if key not in initial_data]
                if missing_keys:
                    raise ValueError(f"Failed to load required sheets: {missing_keys}")
                    
                print("🔥📦 Database sheets loaded successfully. \n")
            except Exception as e:
                print(f"❌ Error loading sheets: {e}")
                raise

        with pd.ExcelFile(ringlist) as rl:
            try:
                sheet_names = rl.sheet_names
                sheet_used = ['Site List', 'Insert Ring']

                for sheet in sheet_used:
                    best_match, score = find_best_match(sheet, sheet_names)
                    if best_match:
                        print(f"Best match for '{sheet}': {best_match} | Score: {score:.2f}")
                        match sheet:
                            case 'Site List':
                                ring_sitelist = pd.read_excel(rl, sheet_name=best_match, na_filter=True)
                                ring_sitelist = sanitize_header(ring_sitelist)
                                initial_data['ring_sitelist'] = ring_sitelist
                            case 'Insert Ring':
                                ring_insertring = pd.read_excel(rl, sheet_name=best_match)
                                ring_insertring = sanitize_header(ring_insertring)
                                initial_data['ring_insertring'] = ring_insertring
                        print(f"✅ {sheet} loaded successfully from '{best_match}'")
                    else:
                        print(f"No suitable match found for '{sheet}'")
                        raise ValueError(f"Sheet '{sheet}' not found in the ring list.")
            except Exception as e:
                print(f"❌ Error loading ring list data: {e}")
                raise

        print("Processing Dummy Database ...")
        print(f"Total sites in ringlist     : {len(ring_sitelist):,}")
        print(f"Total rings in ringlist     : {len(ring_insertring):,}")
        print(f"Total sites in database     : {len(db_sitelist):,}")
        print(f"Total rings in database     : {len(db_newring):,}")
        print(f"Total lengths in database   : {len(db_length):,}")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise
    return initial_data

def insert_to_access(ring_data: pd.DataFrame) -> pd.DataFrame:
    try:
        ring_data = ring_data.copy()
        ring_data['Priority_1'] = ring_data['Priority_1'].apply(lambda x: 'Access' if str(x).lower() == 'insert site' else x)
        ring_data['Priority_2'] = ring_data['Priority_2'].apply(lambda x: 'Access' if str(x).lower() == 'insert site' else x)
        print("✅ Insert Ring converted to Access format.")
        return ring_data
    except Exception as e:
        print(f"❌ Error normalizing Insert Ring: {e}")
        raise

def process_dummy_database(initial_data: dict, dummy_filename:str, export_dir:str = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Export\Streamlit_Result\Dummy_Database"):
    try:
        db_sitelist = initial_data['db_sitelist']
        db_length = initial_data['db_length']
        db_rings = initial_data['db_newring']

        ringsite = initial_data['ring_sitelist']
        ringinsert = initial_data['ring_insertring']
        date_today = str(date.today().strftime('%Y-%m-%d'))

        db_rings = db_rings.reset_index(drop=True)
        db_length = db_length.reset_index(drop=True)
        db_sitelist = db_sitelist.reset_index(drop=True)

        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
            print(f"Export directory created: {export_dir}")
        dummy_filename = os.path.join(export_dir, dummy_filename)

        # CONVERT INSERT RING TO ACCESS
        # db_rings = insert_to_access(db_rings)

        # CREATE DUMMY DATAFRAMES
        dummy_rings = pd.DataFrame(columns=db_rings.columns)
        dummy_length = pd.DataFrame(columns=db_length.columns)
        dummy_sitelist = pd.DataFrame(columns=db_sitelist.columns)

        # COLUMN INSERT SEGMENT
        column_ring = find_best_match('Ring ID', ringinsert.columns.tolist())[0]
        column_site = find_best_match('Site ID', ringinsert.columns.tolist())[0]
        column_ne = find_best_match('Near End', ringinsert.columns.tolist())[0]
        column_fe = find_best_match('Far End', ringinsert.columns.tolist())[0]

        # COLUMN SITE LIST
        column_sitelist_site_id = find_best_match('Site ID IOH', ringsite.columns.tolist())[0]
        column_sitelist_site_name = find_best_match('Site Name', ringsite.columns.tolist())[0]
        column_longitude = find_best_match('Long', ringsite.columns.tolist())[0]
        column_latitude = find_best_match('Lat', ringsite.columns.tolist())[0]

        # COLUMN_DB RING
        column_db_ring = find_best_match('Ring ID', db_rings.columns.tolist())[0]
        column_db_origin = find_best_match('Origin Site ID', db_rings.columns.tolist())[0]
        column_db_destination = find_best_match('Destination', db_rings.columns.tolist())[0]

        # COLUMN_LENGTH
        column_length_ring = find_best_match('Ring ID', db_length.columns.tolist())[0]
        column_length_distance = find_best_match('FO Distance (Meter)', db_length.columns.tolist())[0]
        column_length_avg = find_best_match('AVG Length', db_length.columns.tolist())[0]

        ringlist = ringinsert[column_ring].unique().tolist()
        not_found_rings = pd.DataFrame(columns=['Ring ID', 'Reason'])

        # PROCESS RING INSERTION
        for ring_id in ringlist:
            print(f"Processing Ring ID: {ring_id}")
            source_data = ringinsert[ringinsert[column_ring] == ring_id]
            target_data = db_rings[db_rings[column_db_ring] == ring_id]

            origin_set = set(target_data[column_db_origin])
            destination_set = set(target_data[column_db_destination])
            ne_site_ids = source_data[column_ne].dropna().unique().tolist()
            fe_site_ids = source_data[column_fe].dropna().unique().tolist()
            print(f"Origin Exists: {origin_set} | Destination Exists: {destination_set}")

            if source_data.empty:
                print(f"❌ No data found for Ring ID: {ring_id} in the ring list. Skipping.")
                not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': [f'No data found for Ring ID: {ring_id}']})], ignore_index=True)
                continue

            if target_data.empty:
                print(f"❌ Ring ID: {ring_id} not found in the database. Skipping.")
                not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': [f'Ring ID: {ring_id} not found in the database']})], ignore_index=True)
                continue

            top_part = pd.DataFrame()
            bottom_part = pd.DataFrame()
            new_ring = pd.DataFrame(columns=db_rings.columns)
            
            for idx, row in source_data.iterrows():
                site_id = row[column_site] if column_site in row else None
                if site_id is None:
                    print(f"❌ Site ID is missing for Ring ID: {ring_id}. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Site ID is missing']})], ignore_index=True)
                    continue

                near_end = row[column_ne] if column_ne in row else None
                far_end = row[column_fe] if column_fe in row else None

                if near_end is None or far_end is None:
                    print(f"❌ Near End or Far End is missing for Ring ID: {ring_id}. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': [f'Near End or Far End is missing']})], ignore_index=True)
                    continue
                elif near_end not in origin_set and near_end not in destination_set:
                    print(f"❌ Near End Site ID: {near_end} not found in existing connections. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Near End Site ID not found in existing connections']})], ignore_index=True)
                    continue
                
                near_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == near_end] if near_end in db_sitelist[column_sitelist_site_id].values else pd.DataFrame()
                far_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == far_end] if far_end in db_sitelist[column_sitelist_site_id].values else pd.DataFrame()

                if near_end_data.empty:
                    print(f"❌ Near End Site ID: {near_end} not found in the site list. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Near End Site ID not found in site list']})], ignore_index=True)
                    continue
                elif far_end_data.empty:
                    print(f"❌ Far End Site ID: {far_end} not found in the site list. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Far End Site ID not found in site list']})], ignore_index=True)
                    continue
                elif near_end_data.empty and far_end_data.empty:
                    print(f"❌ Both Near End and Far End Site IDs not found in the site list for Ring ID: {ring_id}. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Both Near End and Far End Site IDs not found in site list']})], ignore_index=True)
                    continue


                # PARTITION RING EXISTING
                start_idx_df = target_data[target_data[column_db_origin] == near_end]
                end_idx_df = target_data[target_data[column_db_destination] == far_end]

                start_index = start_idx_df.index[0] if not start_idx_df.empty else None
                end_index = end_idx_df.index[-1] if not end_idx_df.empty else None

                if start_index is None or end_index is None:
                    print(f"❌ Start or End index not found for Ring ID: {ring_id} | Near End: {near_end} | Far End: {far_end}. Skipping row.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['Start or End index not found']})], ignore_index=True)
                    continue

                top_part = target_data.iloc[:start_index]
                bottom_part = target_data.iloc[end_index + 1:] if end_index + 1 < len(target_data) else pd.DataFrame()
                
                new_data = pd.DataFrame(columns=db_rings.columns)
                for iter in range(2):
                    if iter == 0:
                        origin = near_end
                        destination = site_id
                        priority_1 = 'Access'
                        priority_2 = 'Insert Site'
                        sitename_1 = near_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in near_end_data.columns else None
                        sitename_2 = row[column_sitelist_site_name] if column_sitelist_site_name in row else None
                        long_1 = near_end_data[column_longitude].values[0] if column_longitude in near_end_data.columns else None
                        lat_1 = near_end_data[column_latitude].values[0] if column_latitude in near_end_data.columns else None
                        long_2 = row['Long'] if 'Long' in row else row['Long_1'] if 'Long_1' in row else None
                        lat_2 = row['Lat'] if 'Lat' in row else row['Lat_1'] if 'Lat_1' in row else None
                        existing_cable = row['Existing Cable (m)'] if 'Existing Cable (m)' in row else row['Existing Cable (m)_1'] if 'Existing Cable (m)_1' in row else None
                        new_cable = row['New Cable (m)'] if 'New Cable (m)' in row else row['New Cable (m)_1'] if 'New Cable (m)_1' in row else None
                        total_distance = existing_cable + new_cable if pd.notna(existing_cable) and pd.notna(new_cable) else None

                        print(f"Near End: {near_end} | Far End: {far_end} | Origin: {origin} | Destination: {destination} | Priority 1: {priority_1} | Priority 2: {priority_2}")
                    elif iter == 1:
                        origin = site_id
                        destination = far_end
                        priority_1 = 'Insert Site'
                        priority_2 = 'Access'
                        sitename_1 = row[column_sitelist_site_name] if column_sitelist_site_name in row else None
                        sitename_2 = near_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in near_end_data.columns else None
                        long_1 = row['Long'] if 'Long' in row else row['Long_2'] if 'Long_2' in row else None
                        lat_1 = row['Lat'] if 'Lat' in row else row['Lat_2'] if 'Lat_2' in row else None
                        long_2 = near_end_data[column_longitude].values[0] if column_longitude in near_end_data.columns else None
                        lat_2 = near_end_data[column_latitude].values[0] if column_latitude in near_end_data.columns else None
                        existing_cable = row['Existing Cable (m)_2'] if 'Existing Cable (m)_2' in row else None
                        new_cable = row['New Cable (m)_2'] if 'New Cable (m)_2' in row else None
                        total_distance = existing_cable + new_cable if pd.notna(existing_cable) and pd.notna(new_cable) else None
                        print(f"Near End: {near_end} | Far End: {far_end} | Origin: {origin} | Destination: {destination} | Priority 1: {priority_1} | Priority 2: {priority_2}")
                    else:
                        print(f"❌ Invalid iteration index: {iter}. Skipping row.")
                        continue

                    if origin in origin_set and destination in destination_set:
                        print(f"🔄 Existing         | {origin} → {destination} | Ring ID: {ring_id}. Updating existing data.")
                    elif origin in origin_set and destination not in destination_set:
                        print(f"✅ New destination  | {origin} → {destination} | Ring ID: {ring_id}. Adding new data.")
                    elif origin not in origin_set and destination in destination_set:
                        print(f"✅ New origin       | {origin} → {destination} | Ring ID: {ring_id}. Adding new data.")
                    elif origin not in origin_set and destination not in destination_set:
                        print(f"✅ New connection   | {origin} → {destination} | Ring ID: {ring_id}. Adding new data.")
                    else:
                        print(f"⚠️ No valid connection found for Ring ID: {ring_id} | Origin: {origin} | Destination: {destination}. Skipping row.")
                        not_found_rings[ring_id] = f"No valid connection found for Origin: {origin} | Destination: {destination}."
                        continue

                    for col in db_rings.columns:
                        match col:
                            case 'Ring ID':
                                new_data.loc[iter, col] = ring_id
                            case 'Origin Site ID':
                                new_data.loc[iter, col] = origin
                            case 'Destination':
                                new_data.loc[iter, col] = destination
                            case 'Origin_Name':
                                new_data.loc[iter, col] = sitename_1
                            case 'Destination_Name':
                                new_data.loc[iter, col] = sitename_2
                            case 'Existing Cable (m)':
                                new_data.loc[iter, col] = existing_cable if pd.notna(existing_cable) else 0
                            case 'New Cable (m)':
                                new_data.loc[iter, col] = new_cable if pd.notna(new_cable) else 0
                            case 'Total Distance (m)':
                                new_data.loc[iter, col] = total_distance if pd.notna(total_distance) else 0
                            case 'Vendor':
                                new_data.loc[iter, col] = row.get('Vendor', None)
                            case 'Link Name':
                                new_data.loc[iter, col] = f"{origin}-{destination}"
                            case 'Priority_1':
                                new_data.loc[iter, col] = priority_1
                            case 'Priority_2':
                                new_data.loc[iter, col] = priority_2
                            case 'Long_1':
                                new_data.loc[iter, col] = long_1
                            case 'Lat_1':
                                new_data.loc[iter, col] = lat_1
                            case 'Long_2':
                                new_data.loc[iter, col] = long_2
                            case 'Lat_2':
                                new_data.loc[iter, col] = lat_2
                            case 'Existing/New Site_1':
                                new_data.loc[iter, col] = row.get('Existing/New Site_1', 'New Site')
                            case 'Existing/New Site_2':
                                new_data.loc[iter, col] = row.get('Existing/New Site_2', 'New Site')
                            case 'Ring Status':
                                new_data.loc[iter, col] = row.get('Ring Status', 'new ring')
                            case _:
                                if col in row:
                                    new_data.loc[iter, col] = row[col]
                                else:
                                    best_match, score = find_best_match(col, source_data.columns.tolist())
                                    if best_match and best_match in row:
                                        new_data.loc[iter, col] = row[best_match]
                                    else:
                                        new_data.loc[iter, col] = None

                    new_data.loc[iter, 'date_updated'] = str(date.today().strftime('%Y-%m-%d'))
                    origin_set.add(origin)
                    destination_set.add(destination)

                if new_data.empty:
                    print(f"❌ No new data found for Ring ID: {ring_id}. Skipping.")
                    continue

                if top_part.empty and bottom_part.empty:
                    print(f"❌ No existing data found for Ring ID: {ring_id}. Skipping insertion.")
                    continue
                if top_part is not None and bottom_part is not None:
                    print(f"🔄 Existing data found for Ring ID: {ring_id}. Inserting new data into existing ring.")
                    new_ring = pd.concat([top_part, new_data, bottom_part], ignore_index=True)
                elif top_part is not None:
                    print(f"🔄 Existing data found for Ring ID: {ring_id}. Inserting new data with the top.")
                    new_ring = pd.concat([top_part, new_data], ignore_index=True)
                elif bottom_part is not None:
                    print(f"🔄 Existing data found for Ring ID: {ring_id}. Inserting new data with the bottom.")
                    new_ring = pd.concat([new_data, bottom_part], ignore_index=True)
                else:
                    print(f"❌ No valid data to insert for Ring ID: {ring_id}. Skipping.")
                    not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['No valid data to insert']})], ignore_index=True)
                    continue


            # Check if new_ring is empty
            if new_ring.empty:
                print(f"❌ No new data found for Ring ID: {ring_id}. Skipping insertion.")
                not_found_rings = pd.concat([not_found_rings, pd.DataFrame({'Ring ID': [ring_id], 'Reason': ['No new data found']})], ignore_index=True)
                continue

            dummy_rings = pd.concat([dummy_rings, new_ring], ignore_index=True)

            if dummy_rings.empty:
                print(f"❌ No new data for Ring ID: {ring_id}. Skipping insertion.")
                continue

            print(f"✅ New data for Ring ID: {ring_id} processed successfully.\n")

        # UPDATE LENGTH
        ringlist = dummy_rings[column_db_ring].unique().tolist()
        for ring_id in ringlist:
            source_data = dummy_rings[dummy_rings[column_db_ring] == ring_id]
            if source_data.empty:
                print(f"❌ No data found for Ring ID: {ring_id}. Skipping update.")
                continue

            total_distance = source_data['Total Distance (m)'].sum() if 'Total Distance (m)' in source_data.columns else 0
            total_segments = len(source_data) - 1 if len(source_data) > 1 else 1
            average_length = total_distance / total_segments if total_segments > 0 else 0

            new_length = pd.DataFrame(columns=db_length.columns)

            for col in dummy_length.columns:
                match col:
                    case 'Ring ID':
                        new_length.loc[idx, col] = ring_id
                    case '#of Site':
                        new_length.loc[idx, col] = total_segments
                    case 'FO Distance (Meter)':
                        new_length.loc[idx, col] = total_distance
                    case 'AVG Length':
                        new_length.loc[idx, col] = average_length
                    case 'Vendor':
                        new_length.loc[idx, col] = row.get('Vendor', None)
                    case _:
                        best_match, score = find_best_match(col, source_data.columns.tolist())
                        if best_match and best_match in row:
                            new_length.loc[idx, col] = row[best_match]
                        else:
                            new_length.loc[idx, col] = None
            new_length['date_updated'] = str(date.today().strftime('%Y-%m-%d'))

            if new_length.empty:
                print(f"❌ No new length data found for Ring ID: {ring_id}. Skipping update.")
                continue
            dummy_length = pd.concat([dummy_length, new_length], ignore_index=True)
            print(f"✅ Length data for Ring ID: {ring_id} updated successfully.\n")

        # SITE LIST UPDATE
        for ring_id in ringlist:
            source_data = dummy_rings[dummy_rings[column_db_ring] == ring_id]
            if source_data.empty:
                print(f"❌ No data found for Ring ID: {ring_id}. Skipping site list update.")
                continue

            new_sites = pd.DataFrame(columns=db_sitelist.columns)
            for idx, row in source_data.iterrows():
                site_id = row.get(column_db_origin) or row.get(column_db_destination)
                if site_id in ringsite[column_sitelist_site_id].values:
                    sitelist_info = ringsite[ringsite[column_sitelist_site_id] == site_id]
                elif site_id in db_sitelist['Site ID IOH'].values:
                    sitelist_info = db_sitelist[db_sitelist['Site ID IOH'] == site_id]
                else:
                    print(f"❌ Site ID {site_id} not found in sitelist or existing site list. Skipping row.")
                    continue
                
                if sitelist_info.empty:
                    print(f"❌ No sitelist info found for Site ID: {site_id}. Skipping row.")
                    continue

                for col in dummy_sitelist.columns:
                    match col:
                        case 'Site ID' | 'Site ID IOH':
                            new_sites.loc[idx, col] = site_id
                        case 'Site Name':
                            new_sites.loc[idx, col] = row.get('Origin_Name', None)
                        case 'Program Name':
                            new_sites.loc[idx, col] = row.get('Program', None)
                        case 'Program Ring':
                            new_sites.loc[idx, col] = row.get('Program Ring', None)
                        case 'Program Status':
                            new_sites.loc[idx, col] = row.get('Existing/New Site_1', 'New Site')
                        case 'insert/new ring':
                            new_sites.loc[idx, col] = row.get('Ring Status', "new ring")
                        case 'SoW' | 'Site Owner' | 'Initial Site ID' | 'Initial Site Name':
                            if not sitelist_info.empty:
                                new_sites.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                            else:
                                print(f"❌ No sitelist info found for Site ID: {site_id} | Column: {col}")
                                new_sites.loc[idx, col] = None
                        case _:
                            if col in row:
                                new_sites.loc[idx, col] = row[col]
                            elif col in sitelist_info.columns:
                                new_sites.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
                                # print(f"Using sitelist info for column '{col}'")
                            else:
                                best_match, score = find_best_match(col, dummy_sitelist.columns.tolist())
                                if best_match and best_match in row:
                                    print(f"Using best match '{best_match}' for column '{col}'")
                                    new_sites.loc[idx, col] = row[best_match]
                                else:
                                    print(f"⚠️ No match found for column '{col}' in new site data.")
                                    new_sites.loc[idx, col] = None
                new_sites.loc[idx, 'date_updated'] = str(date.today().strftime('%Y-%m-%d'))

            if new_sites.empty:
                print(f"❌ No new site data found for Ring ID: {ring_id}. Skipping site list update.")
                continue

            dummy_sitelist = pd.concat([dummy_sitelist, new_sites], ignore_index=True)
            print(f"✅ Site list for Ring ID: {ring_id} updated successfully.\n")

        print("-" * 25)
        print("Summary of Dummy Database Update")
        print("-" * 25)
        print(f"Total Rings Processed: {len(dummy_rings):,}")
        print(f"Total Not Found Rings: {len(not_found_rings):,}")
        print(f"Total Lengths Processed: {len(dummy_length):,}")
        print(f"Total Sites Processed: {len(dummy_sitelist):,}\n")

        # STYLIZE RING
        dummy_sitelist = stylize_sitelist(dummy_sitelist)
        dummy_length = stylize_length(dummy_length)
        dummy_rings = stylize_ring(dummy_rings)

        # EXPORT TO EXCEL
        with pd.ExcelWriter(dummy_filename, engine='openpyxl') as writer:
            dummy_sitelist.to_excel(writer, sheet_name='Site List', index=False)
            dummy_length.to_excel(writer, sheet_name='Length', index=False)
            dummy_rings.to_excel(writer, sheet_name='New Ring', index=False)
            not_found_rings.to_excel(writer, sheet_name='Not Found Rings', index=False)

            if 'db_notused' in initial_data:
                not_used = initial_data['db_notused']
                for sheet_name, data in not_used.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"✅ Exported unused sheet: {sheet_name}")

            print(f"🔥👍 Dummy database exported to {dummy_filename}")
        return {
            'file_location': dummy_filename,
            'dummy_rings': dummy_rings,
            'dummy_length': dummy_length,
            'dummy_sitelist': dummy_sitelist,
            'not_found_rings': not_found_rings
        }
    except Exception as e:
        print(f"❌ Error processing dummy database: {e}")
        raise

if __name__ == "__main__":
    database = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\20250702-Drop Site.xlsx"
    ringlist = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Data\Ring List Dummy.xlsx"
    date_today = str(date.today().strftime('%Y%m%d'))
    week = detect_week(date_today)
    dummy_filename = f"{date_today}-Week {week}-Dummy Database.xlsx"
    try:
        initial_data = load_dummy_data(database, ringlist)
        dummy_filename = process_dummy_database(initial_data, dummy_filename)
        print(f"Dummy database created successfully: {dummy_filename}")
    except Exception as e:
        print(f"❌ Error in main execution: {e}")