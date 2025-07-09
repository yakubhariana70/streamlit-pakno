import pandas as pd

def rename_kml_field(kml_content: str, attribute_df: pd.DataFrame, checked_field: str, export_path: str = None) -> str:
    """Renames fields in a KML file based on a mapping DataFrame.
    Args:
        kml_content (str): The content of the KML file as a string.
        attribute_df (pd.DataFrame): DataFrame containing 'before' and 'after' columns for renaming.
        checked_field (str): The field to check for renaming.
        export_path (str, optional): Path to save the revised KML file. If None, returns the revised content.
    Returns:
        str: The revised KML content or the path to the saved file.
    """
    kml_content = kml_content.strip()
    if not kml_content:
        raise ValueError("KML content is empty or not provided.")

    normalized_field = str(checked_field).lower().replace('_', ' ').replace('  ', ' ').strip()

    revised_lines = []
    for line in kml_content.splitlines():
        line_to_check = line.lower().replace('_', ' ')

        if not normalized_field or normalized_field in line_to_check:
            for _, row in attribute_df.iterrows():
                line = line.replace(str(row['before']), str(row['after']))

        revised_lines.append(line)

    revised_kml = '\n'.join(revised_lines)

    if export_path:
        with open(export_path, 'w', encoding='utf-8') as file:
            file.write(revised_kml)
        print(f"Revised KML saved to {export_path}")
        return export_path
    else:
        print("No export path provided, returning revised KML content.")
    return revised_kml
    
if __name__ == "__main__":
    # Example usage
    kml_path = r"example.kml"  # Replace with your KML file path
    attribute_path = r"mapping.csv"  # Replace with your mapping CSV file path
    checked_field = 'site id'  # The field to check for renaming

    # Load the mapping DataFrame
    attribute_df = pd.read_csv(attribute_path)

    # Read the KML content
    with open(kml_path, 'r', encoding='utf-8') as file:
        kml_content = file.read()

