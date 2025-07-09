import pandas as pd
import jellyfish as jf
import os
import re

def find_best_match(word, candidates, threshold=0.85):
    best_match = None
    best_score = 0
    for candidate in candidates:
        score = jf.jaro_winkler_similarity(word, candidate)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate
    return best_match, best_score

def sanitize_header(df, preview_row = 5):
    if df.columns[0].startswith('Unnamed'):
        for idx, row in df.head(preview_row).iterrows():
            if pd.isna(row.values[0]) or row.values[0] == '':
                continue
            else:
                df.columns = [str(col).strip() for col in row]
                df = df.iloc[idx + 1:].reset_index(drop=True)
                print(f"Header sanitized | Start from row {idx + 1} | Columns: {[col.strip() for col in df.columns[:3]]} ...")
                break
    else:
        df.columns = [col.strip() for col in df.columns]
    
    # Drop nan columns
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    df = df.loc[:, ~df.columns.str.contains('nan', na=False)]
    
    # Clean entered columns
    df.columns = [col.split('.')[0] for col in df.columns]
    df.columns = [col.strip().replace('\n', '') for col in df.columns]
    
    # Clean duplicate columns
    duplicated_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicated_cols:
        new_columns = []
        col_count = {}
        for col in df.columns:
            if col in duplicated_cols:
                col_count[col] = col_count.get(col, 0) + 1
                new_col_name = f"{col}_{col_count[col]}"
                new_columns.append(new_col_name)
            else:
                new_columns.append(col)
        print(f"‼️ Duplicate columns found")
        for col in duplicated_cols:
            print(f"  - {col} | Total: {col_count[col]}")
        df.columns = new_columns
    else:
        print("No duplicate columns found.")
    return df


def detect_week(date_str):
    try:
        date_obj = pd.to_datetime(date_str, format='%Y%m%d')
        week_number = date_obj.isocalendar()[1]
        return week_number
    except ValueError:
        print(f"❌ Invalid date format: {date_str}")
        return None

def detect_version(filepath: str | os.PathLike) -> str:
    filename = str(filepath).lower().split(os.sep)[-1]
    print(f"Detecting version from filename: {filename}")
    search_version = re.search(r'v(?P<version>\d+)', filename)
    if search_version:
        version = search_version.group('version')
        new_version = f"v{int(version) + 1}"
        print(f"Version detected: {version} | New version will be: {new_version}")
        return new_version
    else:
        print("No version detected in the filename.")
        return "v1"
    
def stylize_sitelist(sitelist_data: pd.DataFrame):
    try:
        # Set Style
        stylized = (
            sitelist_data
            .style
            .map_index(lambda _: "background-color: red; color: white; font-weight: bold;", axis=1)
            .map(lambda v: 'border: 1px solid black')
            .format(precision=8, thousands=',', decimal='.')
        )

        return stylized
    except Exception as e:
        print(f"❌ Error in stylizing sitelist data: {e}")
        return sitelist_data.style
    
def stylize_length(length_data: pd.DataFrame) :
    try:
        # Set Style
        stylized = (
            length_data
            .style
            .format(precision=8, thousands=',', decimal='.')
            .map_index(lambda _: "background-color: #FFC000; color: black; font-weight: bold;", axis=1)
            .map(lambda v: 'border: 1px solid black')
        )

        return stylized
    except Exception as e:
        print(f"❌ Error in stylizing length data: {e}")
        return length_data.style


def stylize_ring(ring_data: pd.DataFrame) :
    try:
        origin_cols = ring_data.columns[ring_data.columns.get_loc('Origin Site ID'):ring_data.columns.get_loc('Destination')-1]
        destination_cols = ring_data.columns[ring_data.columns.get_loc('Destination'):ring_data.columns.get_loc('Priority_2')+1]
        p0_origin = ring_data.loc[:, "Priority_1"].astype(str) == 'P0'
        p0_destination = ring_data.loc[:, "Priority_2"].astype(str) == 'P0'
        
        # Set Style
        stylized = (
            ring_data
            .style
            .map_index(lambda _: "background-color: red; color: white; font-weight: bold;", axis=1)
            .map(lambda v: 'background-color: yellow' if str(v).lower() == 'insert site' else '')
            .apply(lambda _: ['background-color: #add8e6' if p0_origin[i] else ''
                              for i in range(len(ring_data))], axis=0, subset=origin_cols)
            .apply(lambda _: ['background-color: #add8e6' if p0_destination[i] else ''
                              for i in range(len(ring_data))], axis=0, subset=destination_cols)
            .map(lambda v: 'border: 1px solid black')
            .format(precision=8, thousands=',', decimal='.')
            .set_table_attributes('style="width: 100%; border-collapse: collapse;"')
        )

        return stylized
    except Exception as e:
        print(f"❌ Error in stylizing ring data: {e}")
        return ring_data.style

