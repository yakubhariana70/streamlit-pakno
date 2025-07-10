import pandas as pd
import os
from datetime import date
from modules.utils import find_best_match, sanitize_header, detect_week, stylize_ring, stylize_length, stylize_sitelist
from tqdm import tqdm

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
                print("✅ All required sheets loaded successfully.")

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

        print("\nProcessing Dummy Database ...")
        print(f"Total sites in ringlist     : {len(ring_sitelist):,}")
        print(f"Total rings in ringlist     : {len(ring_insertring):,}")
        print(f"Total sites in database     : {len(db_sitelist):,}")
        print(f"Total rings in database     : {len(db_newring):,}")
        print(f"Total lengths in database   : {len(db_length):,}\n")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise
    return initial_data

def insert_to_access(ring_data: pd.DataFrame) -> pd.DataFrame:
    try:
        ring_data = ring_data.copy()
        ring_data['Priority_1'] = ring_data['Priority_1'].apply(lambda x: 'Access' if str(x).lower() == 'insert site' else x)
        ring_data['Priority_2'] = ring_data['Priority_2'].apply(lambda x: 'Access' if str(x).lower() == 'insert site' else x)
        print("✅ Insert Ring converted to Access format.\n")
        return ring_data
    except Exception as e:
        print(f"❌ Error normalizing Insert Ring: {e}\n")
        raise

def process_dummy_database(initial_data: dict, dummy_filename:str, export_dir:str = r"D:\Data Analytical\PROJECT\REQUEST\20250626_Automate DB Update IOH\Export\Streamlit_Result\Dummy_Database"):
    try:
        db_sitelist = initial_data['db_sitelist']
        db_length = initial_data['db_length']
        db_rings = initial_data['db_newring']

        ringsite = initial_data['ring_sitelist']
        ringinsert = initial_data['ring_insertring']

        db_rings = db_rings.reset_index(drop=True)
        db_length = db_length.reset_index(drop=True)
        db_sitelist = db_sitelist.reset_index(drop=True)

        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
            print(f"Export directory created: {export_dir}")
        dummy_filename = os.path.join(export_dir, dummy_filename)

        # CONVERT INSERT RING TO ACCESS
        db_rings = insert_to_access(db_rings)

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
        insert_site_ids = ringinsert[column_site].dropna().unique().tolist()

        # ==========================
        # PROCESS RING INSERTION
        # ==========================
        error_log = {
            'errors': [],
            'warnings': [],
            'info': []
        }
        for ring_id in tqdm(ringlist, desc="🛟 Processing Rings", unit="ring"):
            print(f"Ring ID | {ring_id}")
            source_data = ringinsert[ringinsert[column_ring] == ring_id].copy()
            target_data = db_rings[db_rings[column_db_ring] == ring_id].copy()

            # DETECT ENTRIES
            if len(source_data) > 1:
                print(f"ℹ️ Multiple entries found for Ring ID: {ring_id}.")
            else:
                print(f"ℹ️ Single entry found for Ring ID: {ring_id}.")

            origin_set = set(target_data[column_db_origin])
            destination_set = set(target_data[column_db_destination])
            ne_site_ids = source_data[column_ne].dropna().unique().tolist()
            fe_site_ids = source_data[column_fe].dropna().unique().tolist()

            print(f"Origin      : {len(origin_set)} | {origin_set}")
            print(f"Destination : {len(destination_set)} | {destination_set}")

            if source_data.empty:
                print(f"❌ No data found for Ring ID: {ring_id} in the ring list. Skipping.\n")
                error_log['warnings'].append(f"RING | No data found for Ring ID: {ring_id} in the ring list.")
                continue

            if target_data.empty:
                print(f"❌ Ring ID: {ring_id} not found in the database. Skipping.\n")
                error_log['warnings'].append(f"RING | Ring ID: {ring_id} not found in the database.")
                continue
            
            # PARTITION DATA
            top_part = pd.DataFrame()
            bottom_part = pd.DataFrame()
            new_ring = pd.DataFrame(columns=db_rings.columns)
            
            # Find insertion points
            start_location = target_data[target_data[column_db_origin].isin(ne_site_ids)]
            end_location = target_data[target_data[column_db_destination].isin(fe_site_ids)]

            start_index = None
            end_index = None
            start_position = None
            end_position = None

            if not start_location.empty:
                start_index = start_location.index[0]
                # Convert DataFrame index to positional index
                start_position = target_data.index.get_loc(start_index)
                top_part = target_data.iloc[:start_position]
                print(f"✅ Top part found for Ring ID: {ring_id} | NE: {ne_site_ids} | Index: {start_index} | Position: {start_position}")
            else:
                print(f"❌ No top part found for Ring ID: {ring_id} | NE: {ne_site_ids}")

            if not end_location.empty:
                end_index = end_location.index[-1]
                # Convert DataFrame index to positional index
                end_position = target_data.index.get_loc(end_index)
                bottom_part = target_data.iloc[end_position + 1:] if end_position + 1 < len(target_data) else pd.DataFrame()
                print(f"✅ Bottom part found for Ring ID: {ring_id} | FE: {fe_site_ids} | Index: {end_index} | Position: {end_position}")
            else:
                print(f"❌ No bottom part found for Ring ID: {ring_id} | FE: {fe_site_ids}")

            # Handle edge cases for partitioning
            if start_position is None and end_position is not None:
                print(f"⚠️ No NE connection found. Inserting before FE at position {end_position}")
                top_part = target_data.iloc[:end_position]
                bottom_part = target_data.iloc[end_position:]
            elif end_position is None and start_position is not None:
                print(f"⚠️ No FE connection found. Inserting after NE at position {start_position}")
                top_part = target_data.iloc[:start_position + 1]
                bottom_part = target_data.iloc[start_position + 1:]
            elif start_position is None and end_position is None:
                print(f"⚠️ No insertion points found. Appending to end of ring.")
                top_part = target_data.copy()
                bottom_part = pd.DataFrame()
            elif start_position is not None and end_position is not None:
                if start_position > end_position:
                    print(f"⚠️ Start position ({start_position}) > End position ({end_position}). Swapping positions.")
                    start_position, end_position = end_position, start_position
                print(f"✅ Inserting between positions {start_position} and {end_position + 1}")
                top_part = target_data.iloc[:start_position + 1]  # Include the NE connection
                bottom_part = target_data.iloc[end_position:]      # Include the FE connection
            
            insert_data = pd.DataFrame(columns=db_rings.columns)
            
            # Process all insert sites for this ring at once to create proper chaining
            insert_sites = source_data[column_site].dropna().tolist()
            
            if len(insert_sites) > 1:
                print(f"🔗 Multiple insert sites found: {insert_sites}. Creating chain connections.")
            else:
                print(f"🔗 Single insert site found: {insert_sites}.")
            
            # Get the first row to extract common data (NE, FE, etc.)
            first_row = source_data.iloc[0]
            near_end = first_row[column_ne] if column_ne in first_row else None
            far_end = first_row[column_fe] if column_fe in first_row else None
            
            if near_end is None or far_end is None:
                print(f"❌ NE or FE is missing for Ring ID: {ring_id}. Skipping.")
                error_log['errors'].append(f"RING | NE or FE is missing for Ring ID: {ring_id}.")
                continue
            
            # Get site data for NE and FE
            near_in_source = near_end in insert_site_ids
            far_in_source = far_end in insert_site_ids
            
            near_end_data = pd.DataFrame()
            far_end_data = pd.DataFrame()
            
            if near_in_source:
                near_end_data = ringsite[ringsite[column_sitelist_site_id] == near_end]
                if near_end_data.empty:
                    near_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == near_end]
            else:
                near_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == near_end]
                
            if far_in_source:
                far_end_data = ringsite[ringsite[column_sitelist_site_id] == far_end]
                if far_end_data.empty:
                    far_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == far_end]
            else:
                far_end_data = db_sitelist[db_sitelist[column_sitelist_site_id] == far_end]
            
            if near_end_data.empty or far_end_data.empty:
                print(f"❌ NE: {near_end} or FE: {far_end} not found in site lists. Skipping.")
                continue
            
            # Create the connection chain: NE → Site1 → Site2 → ... → SiteN → FE
            connection_chain = [near_end] + insert_sites + [far_end]
            print(f"🔗 Connection chain: {' → '.join(connection_chain)}")
            
            # Create a lookup for site data from source
            site_data_lookup = {}
            for _, row in source_data.iterrows():
                site_id = row[column_site]
                if pd.notna(site_id):
                    site_data_lookup[site_id] = row
            
            print(f"🔍 Site data lookup created: {list(site_data_lookup.keys())}")
            
            # If we have multiple insert sites but only one row in source_data,
            # use that row's data for all insert sites
            if len(insert_sites) > 1 and len(site_data_lookup) == 1:
                common_row = source_data.iloc[0]
                for site_id in insert_sites:
                    if site_id not in site_data_lookup:
                        site_data_lookup[site_id] = common_row
                print(f"🔄 Using common row data for all {len(insert_sites)} insert sites")
            elif len(insert_sites) > 1 and len(site_data_lookup) < len(insert_sites):
                # Handle case where we have some but not all site data
                common_row = source_data.iloc[0]
                for site_id in insert_sites:
                    if site_id not in site_data_lookup:
                        site_data_lookup[site_id] = common_row
                print(f"🔄 Filling missing site data for {len(insert_sites) - len(site_data_lookup)} sites")
            
            # Create connections for each link in the chain
            for i in range(len(connection_chain) - 1):
                origin_site = connection_chain[i]
                destination_site = connection_chain[i + 1]
                
                # Get site data for origin and destination
                if origin_site == near_end:
                    origin_data = near_end_data
                    origin_in_source = near_in_source
                elif origin_site == far_end:
                    origin_data = far_end_data
                    origin_in_source = far_in_source
                else:
                    # Get site data from lookup or ringsite/db_sitelist
                    if origin_site in site_data_lookup:
                        origin_row = site_data_lookup[origin_site]
                        origin_data = pd.DataFrame([origin_row])
                        origin_in_source = True
                    else:
                        # Try to find in ringsite or db_sitelist
                        origin_data = ringsite[ringsite[column_sitelist_site_id] == origin_site]
                        if origin_data.empty:
                            origin_data = db_sitelist[db_sitelist[column_sitelist_site_id] == origin_site]
                        origin_in_source = False
                        if origin_data.empty:
                            print(f"⚠️ Warning: Origin site {origin_site} not found in any data source")
                
                if destination_site == near_end:
                    dest_data = near_end_data
                    dest_in_source = near_in_source
                elif destination_site == far_end:
                    dest_data = far_end_data
                    dest_in_source = far_in_source
                else:
                    # Get site data from lookup or ringsite/db_sitelist
                    if destination_site in site_data_lookup:
                        dest_row = site_data_lookup[destination_site]
                        dest_data = pd.DataFrame([dest_row])
                        dest_in_source = True
                    else:
                        # Try to find in ringsite or db_sitelist
                        dest_data = ringsite[ringsite[column_sitelist_site_id] == destination_site]
                        if dest_data.empty:
                            dest_data = db_sitelist[db_sitelist[column_sitelist_site_id] == destination_site]
                        dest_in_source = False
                        if dest_data.empty:
                            print(f"⚠️ Warning: Destination site {destination_site} not found in any data source")
                
                # Determine priorities
                if origin_site == near_end:
                    priority_1 = 'Access' if not origin_in_source else 'Insert Site'
                elif origin_site in insert_sites:
                    priority_1 = 'Insert Site'
                else:
                    priority_1 = 'Access'
                    
                if destination_site == far_end:
                    priority_2 = 'Access' if not dest_in_source else 'Insert Site'
                elif destination_site in insert_sites:
                    priority_2 = 'Insert Site'
                else:
                    priority_2 = 'Access'
                
                # Get connection data - use the first available source row or lookup
                if origin_site in site_data_lookup:
                    conn_row = site_data_lookup[origin_site]
                elif destination_site in site_data_lookup:
                    conn_row = site_data_lookup[destination_site]
                else:
                    conn_row = source_data.iloc[0]  # Fallback to first row
                
                # Extract coordinates and site names
                if origin_site == near_end:
                    long_1 = near_end_data[column_longitude].values[0] if column_longitude in near_end_data.columns else None
                    lat_1 = near_end_data[column_latitude].values[0] if column_latitude in near_end_data.columns else None
                    sitename_1 = near_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in near_end_data.columns else None
                elif origin_site == far_end:
                    long_1 = far_end_data[column_longitude].values[0] if column_longitude in far_end_data.columns else None
                    lat_1 = far_end_data[column_latitude].values[0] if column_latitude in far_end_data.columns else None
                    sitename_1 = far_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in far_end_data.columns else None
                else:
                    # For insert sites, try to get coordinates from multiple sources
                    if origin_site in site_data_lookup:
                        origin_row = site_data_lookup[origin_site]
                        long_1 = origin_row.get('Long', origin_row.get('Long_1', origin_row.get(column_longitude, None)))
                        lat_1 = origin_row.get('Lat', origin_row.get('Lat_1', origin_row.get(column_latitude, None)))
                        sitename_1 = origin_row.get(column_sitelist_site_name, origin_row.get('Site Name', None))
                    else:
                        # Try to get from origin_data if available
                        if not origin_data.empty:
                            long_1 = origin_data[column_longitude].values[0] if column_longitude in origin_data.columns else None
                            lat_1 = origin_data[column_latitude].values[0] if column_latitude in origin_data.columns else None
                            sitename_1 = origin_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in origin_data.columns else None
                        else:
                            long_1 = lat_1 = sitename_1 = None
                
                if destination_site == near_end:
                    long_2 = near_end_data[column_longitude].values[0] if column_longitude in near_end_data.columns else None
                    lat_2 = near_end_data[column_latitude].values[0] if column_latitude in near_end_data.columns else None
                    sitename_2 = near_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in near_end_data.columns else None
                elif destination_site == far_end:
                    long_2 = far_end_data[column_longitude].values[0] if column_longitude in far_end_data.columns else None
                    lat_2 = far_end_data[column_latitude].values[0] if column_latitude in far_end_data.columns else None
                    sitename_2 = far_end_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in far_end_data.columns else None
                else:
                    # For insert sites, try to get coordinates from multiple sources
                    if destination_site in site_data_lookup:
                        dest_row = site_data_lookup[destination_site]
                        long_2 = dest_row.get('Long', dest_row.get('Long_2', dest_row.get(column_longitude, None)))
                        lat_2 = dest_row.get('Lat', dest_row.get('Lat_2', dest_row.get(column_latitude, None)))
                        sitename_2 = dest_row.get(column_sitelist_site_name, dest_row.get('Site Name', None))
                    else:
                        # Try to get from dest_data if available
                        if not dest_data.empty:
                            long_2 = dest_data[column_longitude].values[0] if column_longitude in dest_data.columns else None
                            lat_2 = dest_data[column_latitude].values[0] if column_latitude in dest_data.columns else None
                            sitename_2 = dest_data[column_sitelist_site_name].values[0] if column_sitelist_site_name in dest_data.columns else None
                        else:
                            long_2 = lat_2 = sitename_2 = None
                
                # Cable distances (simplified - use from first connection)
                existing_cable = conn_row.get('Existing Cable (m)', conn_row.get('Existing Cable (m)_1', 0))
                new_cable = conn_row.get('New Cable (m)', conn_row.get('New Cable (m)_1', 0))
                total_distance = existing_cable + new_cable if pd.notna(existing_cable) and pd.notna(new_cable) else 0
                
                print(f"✅ Creating connection: {origin_site} → {destination_site} | Priority: {priority_1} → {priority_2}")
                
                # Create new connection row
                new_connection = pd.DataFrame(columns=db_rings.columns)
                connection_idx = len(insert_data)
                
                for col in db_rings.columns:
                    match col:
                        case 'Ring ID':
                            new_connection.loc[connection_idx, col] = ring_id
                        case 'Origin Site ID':
                            new_connection.loc[connection_idx, col] = origin_site
                        case 'Destination':
                            new_connection.loc[connection_idx, col] = destination_site
                        case 'Origin_Name':
                            new_connection.loc[connection_idx, col] = sitename_1
                        case 'Destination_Name':
                            new_connection.loc[connection_idx, col] = sitename_2
                        case 'Existing Cable (m)':
                            new_connection.loc[connection_idx, col] = existing_cable if pd.notna(existing_cable) else 0
                        case 'New Cable (m)':
                            new_connection.loc[connection_idx, col] = new_cable if pd.notna(new_cable) else 0
                        case 'Total Distance (m)':
                            new_connection.loc[connection_idx, col] = total_distance if pd.notna(total_distance) else 0
                        case 'Vendor':
                            new_connection.loc[connection_idx, col] = conn_row.get('Vendor', None)
                        case 'Link Name':
                            new_connection.loc[connection_idx, col] = f"{origin_site}-{destination_site}"
                        case 'Priority_1':
                            new_connection.loc[connection_idx, col] = priority_1
                        case 'Priority_2':
                            new_connection.loc[connection_idx, col] = priority_2
                        case 'Long_1':
                            new_connection.loc[connection_idx, col] = long_1
                        case 'Lat_1':
                            new_connection.loc[connection_idx, col] = lat_1
                        case 'Long_2':
                            new_connection.loc[connection_idx, col] = long_2
                        case 'Lat_2':
                            new_connection.loc[connection_idx, col] = lat_2
                        case 'Existing/New Site_1':
                            new_connection.loc[connection_idx, col] = conn_row.get('Existing/New Site_1', 'New Site')
                        case 'Existing/New Site_2':
                            new_connection.loc[connection_idx, col] = conn_row.get('Existing/New Site_2', 'New Site')
                        case 'Ring Status':
                            new_connection.loc[connection_idx, col] = conn_row.get('Ring Status', 'new ring')
                        case _:
                            if col in conn_row:
                                new_connection.loc[connection_idx, col] = conn_row[col]
                            else:
                                best_match, score = find_best_match(col, source_data.columns.tolist())
                                if best_match and best_match in conn_row:
                                    new_connection.loc[connection_idx, col] = conn_row[best_match]
                                else:
                                    new_connection.loc[connection_idx, col] = None
                
                new_connection.loc[connection_idx, 'date_updated'] = str(date.today().strftime('%Y-%m-%d'))
                
                # Add this connection to insert_data
                insert_data = pd.concat([insert_data, new_connection], ignore_index=True)

            # Check if we have data to insert
            if insert_data.empty:
                print(f"❌ No valid data to insert for Ring ID: {ring_id}. Skipping.\n")
                error_log['warnings'].append(f"RING | No valid data to insert for Ring ID: {ring_id}.")
                continue

            # Reconstruct the ring with inserted data
            if not top_part.empty and not bottom_part.empty:
                print(f"🔄 Inserting data between NE and FE connections for Ring ID: {ring_id}.")
                print(f"   Top part: {len(top_part)} connections")
                print(f"   Insert data: {len(insert_data)} connections") 
                print(f"   Bottom part: {len(bottom_part)} connections")
                new_ring = pd.concat([top_part, insert_data, bottom_part], ignore_index=True)
            elif not top_part.empty and bottom_part.empty:
                print(f"🔄 Appending data after NE connection for Ring ID: {ring_id}.")
                new_ring = pd.concat([top_part, insert_data], ignore_index=True)
            elif top_part.empty and not bottom_part.empty:
                print(f"🔄 Prepending data before FE connection for Ring ID: {ring_id}.")
                new_ring = pd.concat([insert_data, bottom_part], ignore_index=True)
            else:
                print(f"🔄 Creating new ring with insert data for Ring ID: {ring_id}.")
                new_ring = insert_data.copy()
            
            # Add the new ring to dummy_rings
            dummy_rings = pd.concat([dummy_rings, new_ring], ignore_index=True)
            print(f"👍🔥 New data for Ring ID: {ring_id} processed successfully.\n")

        # ==========================
        # UPDATE LENGTH
        # ==========================
        print("\n♾️ Updating Length Data ...")
        ringlist = dummy_rings[column_db_ring].unique().tolist()
        for ring_id in ringlist:
            source_data = dummy_rings[dummy_rings[column_db_ring] == ring_id]
            if source_data.empty:
                print(f"❌ LENGTH | No data found for Ring ID: {ring_id}. Skipping update.\n")
                error_log['warnings'].append(f"No data found for Ring ID: {ring_id} in dummy rings.")
                continue

            total_distance = source_data['Total Distance (m)'].sum() if 'Total Distance (m)' in source_data.columns else 0
            total_segments = len(source_data) - 1 if len(source_data) > 1 else 1
            average_length = total_distance / total_segments if total_segments > 0 else 0

            new_length = pd.DataFrame(columns=db_length.columns)
            idx = 0  # Initialize index for new row
            
            # Get representative row for vendor and other data
            rep_row = source_data.iloc[0] if not source_data.empty else {}
            
            for col in db_length.columns:
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
                        new_length.loc[idx, col] = rep_row.get('Vendor', None)
                    case _:
                        best_match, score = find_best_match(col, source_data.columns.tolist())
                        if best_match and best_match in rep_row:
                            new_length.loc[idx, col] = rep_row[best_match]
                        else:
                            new_length.loc[idx, col] = None
            new_length['date_updated'] = str(date.today().strftime('%Y-%m-%d'))

            if new_length.empty:
                print(f"❌ LENGTH | No new length data found for Ring ID: {ring_id}. Skipping update.")
                error_log['warnings'].append(f"No new length data found for Ring ID: {ring_id}.")
                continue
            dummy_length = pd.concat([dummy_length, new_length], ignore_index=True)
            print(f"✅ Length data for Ring ID: {ring_id} updated successfully.")

        # ==========================
        # SITE LIST UPDATE
        # ==========================
        print("\n📍 Updating Site List ...")
        for ring_id in ringlist:
            source_data = dummy_rings[dummy_rings[column_db_ring] == ring_id]
            if source_data.empty:
                print(f"❌ No data found for Ring ID: {ring_id}. Skipping site list update. \n")
                error_log['warnings'].append(f"SITE LIST | No data found for Ring ID: {ring_id}.")
                continue

            new_sites = pd.DataFrame(columns=db_sitelist.columns)
            new_sites['date_updated'] = None
            for idx, row in source_data.iterrows():
                site_id = row.get(column_db_origin) or row.get(column_db_destination)
                if site_id in ringsite[column_sitelist_site_id].values:
                    sitelist_info = ringsite[ringsite[column_sitelist_site_id] == site_id]
                elif site_id in db_sitelist['Site ID IOH'].values:
                    sitelist_info = db_sitelist[db_sitelist['Site ID IOH'] == site_id]
                else:
                    print(f"❌ Site ID {site_id} not found in sitelist or existing site list. Skipping row.")
                    error_log['errors'].append(f"SITE LIST | Site ID {site_id} not found in sitelist or existing site list for Ring ID: {ring_id} | Row: {idx}.")
                    continue
                
                if sitelist_info.empty:
                    print(f"❌ No sitelist info found for Site ID: {site_id}. Skipping row.")
                    error_log['errors'].append(f"SITE LIST | No sitelist info found for Site ID: {site_id} for Ring ID: {ring_id} | Row: {idx}.")
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
                                error_log['errors'].append(f"SITE LIST | No sitelist info found for Site ID: {site_id} | Column: {col} for Ring ID: {ring_id} | Row: {idx}.")
                                new_sites.loc[idx, col] = None
                        case _:
                            if col in row:
                                new_sites.loc[idx, col] = row[col]
                            elif col in sitelist_info.columns:
                                new_sites.loc[idx, col] = sitelist_info.iloc[0].get(col, None)
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
                error_log['warnings'].append(f"No new site data found for Ring ID: {ring_id}.")
                continue

            dummy_sitelist = pd.concat([dummy_sitelist, new_sites], ignore_index=True)
            print(f"✅ Site list for Ring ID: {ring_id} updated successfully.\n")

        print("-" * 25)
        print("Summary of Dummy Database Update")
        print("-" * 25)
        print(f"Total Rings Processed: {len(dummy_rings):,}")
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
            print(f"🔥👍 Dummy database exported to {dummy_filename}")
        return {
            'file_location': dummy_filename,
            'dummy_rings': dummy_rings,
            'dummy_length': dummy_length,
            'dummy_sitelist': dummy_sitelist,
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