import pandas as pd
from tqdm import tqdm

# def rename_kml_field(kml_content: str, attribute_df: pd.DataFrame, checked_field: str, export_path: str = None) -> str:
#     """Renames fields in a KML file based on a mapping DataFrame.
#     Args:
#         kml_content (str): The content of the KML file as a string.
#         attribute_df (pd.DataFrame): DataFrame containing 'before' and 'after' columns for renaming.
#         checked_field (str): The field to check for renaming.
#         export_path (str, optional): Path to save the revised KML file. If None, returns the revised content.
#     Returns:
#         str: The revised KML content or the path to the saved file.
#     """
#     kml_content = kml_content.strip()
#     if not kml_content:
#         raise ValueError("KML content is empty or not provided.")

#     normalized_field = str(checked_field).lower().replace('_', ' ').replace('  ', ' ').strip()

#     revised_lines = []
#     for line in kml_content.splitlines():
#         line_to_check = line.lower().replace('_', ' ')

#         if not normalized_field or normalized_field in line_to_check:
#             for _, row in attribute_df.iterrows():
#                 line = line.replace(str(row['before']), str(row['after']))

#         revised_lines.append(line)

#     revised_kml = '\n'.join(revised_lines)

#     if export_path:
#         with open(export_path, 'w', encoding='utf-8') as file:
#             file.write(revised_kml)
#         print(f"Revised KML saved to {export_path}")
#         return export_path
#     else:
#         print("No export path provided, returning revised KML content.")
#     return revised_kml
    
# if __name__ == "__main__":
#     # Example usage
#     kml_path = r"example.kml"  # Replace with your KML file path
#     attribute_path = r"mapping.csv"  # Replace with your mapping CSV file path
#     checked_field = 'site id'  # The field to check for renaming

#     # Load the mapping DataFrame
#     attribute_df = pd.read_csv(attribute_path)

#     # Read the KML content
#     with open(kml_path, 'r', encoding='utf-8') as file:
#         kml_content = file.read()

def rename_kml_field(kml_path: str, attribute_path: str, checked_field: str) -> None:
    """
    Rename fields in a KML file based on a mapping provided in an Excel file.
    
    Parameters:
        kml_path (str): Path to the KML file.
        attribute_path (str): Path to the Excel file containing the mapping of field names to change,
                            file should contain only 1 sheet + before and after column.
        checked_field (str): Column name of the KML file to rename.
    
    Returns:
        str: The revised KML content as a string.
    """
    with open(kml_path, 'r', encoding='utf-8') as file:
        kml_content = file.read()
    attribute_df = pd.read_excel(attribute_path)
    lines = kml_content.split('\n')
    for _, row in tqdm(attribute_df.iterrows(), total = len(attribute_df)):
        lines = [i.replace(str(row['before']), str(row['after'])) if 'name' in i.lower() else i for i in lines]

    kml_revised = '\n'.join(lines)
    return kml_revised
    

kml_path = r""
attribute_path = r""
checked_field = 'site id'
revised_kml = rename_kml_field(kml_path, attribute_path, checked_field)
open(kml_path.replace('.kml','revised.kml'), 'w').write(revised_kml)

