import os
import csv
import json
import xml.etree.ElementTree as ET
import argparse
import sys


class DataProcessor:
    """ Process different types of files (XML, TSV, plain text) to extract address data."""
    def process_xml(self, file_path):
        """
        Parse XML files to extract address data. Handles different tags like NAME, COMPANY, STREET, etc., 
        and normalizes them into a common format. Ensures ZIP code is properly formatted.
        Parameters:
            file_path (str): The path to the XML file to be processed.
        Returns:
            data_list (list of dicts): Parsed data with normalized keys.
        Raises:
            SystemExit: If an error occurs during parsing.
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            data_list = []
            for ent in root.findall(".//ENT"):
                data = {}
                for tag in ["NAME", "COMPANY", "STREET", "CITY", "STATE"]:
                    element = ent.find(tag)
                    if tag == "COMPANY":
                        tag = "organization"
                    if element is not None and element.text.strip() != "":
                        data[tag.lower()] = element.text.strip()
                # Handle ZIP code formatting
                zip_code = ent.find("POSTAL_CODE").text.strip()
                if zip_code:
                    zip_code = zip_code.split("-")
                    data["zip"] = zip_code[0].strip()
                    if len(zip_code) > 1 and zip_code[1]:
                        data["zip"] += "-" + zip_code[1].strip()
                if data:
                    data_list.append(data)
            # Move zip to the last position in the data order
            if "zip" in data_list[0]:
                data_list = sorted(data_list, key=lambda x: "zip" not in x)
            return data_list
        except Exception as e:
            sys.stderr.write(f"Error parsing XML file {file_path}: {str(e)}\n")
            sys.exit(1)

    def process_tsv(self, file_path):
        """
        Parse TSV files to extract address data, ensuring ZIP codes and other data are correctly formatted.
        Parameters:
            file_path (str): The path to the TSV file to be processed.
        Returns:
            data_list (list of dicts): Parsed data with normalized keys.
        Raises:
            SystemExit: If an error occurs during parsing.
        """
        try:
            data_list = []
            with open(file_path, mode="r", encoding="utf-8") as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter="\t")
                for row in reader:
                    data = {}
                    # Extract full name
                    full_name = [row["first"], row["middle"], row["last"]]
                    full_name = [
                        name.strip() for name in full_name if name.strip() != ("N/M/N")
                    ]
                    full_name = " ".join(full_name)
                    if full_name.strip() and not row["last"].endswith(
                        ("LLC", "Inc.", "Ltd.")
                    ):
                        data["name"] = full_name.strip()
                    else:
                        # Extract organization 
                        if row["last"].endswith(("LLC", "Inc.", "Ltd.")):
                            row["organization"] = row["last"]
                        if row["organization"] != "N/A":
                            data["organization"] = row["organization"]
                        # Extract address, city, and state
                        for key in ["address", "city", "state"]:
                            if row[key].strip():
                                data[key] = row[key].strip()
                        # Handle zip code formatting
                        if row["zip4"]:
                            data["zip"] = row["zip"].strip() + "-" + row["zip4"].strip()
                        else:
                            data["zip"] = row["zip"].strip()
                        if data:
                            data_list.append(data)
            return data_list
        except Exception as e:
            sys.stderr.write(f"Error parsing TSV file {file_path}: {str(e)}\n")
            sys.exit(1)

    def process_plain_text(self, file_path):
        """
        Parse plain text files to extract address data, ensuring ZIP code and other data are correctly formatted.
        Parameters:
            file_path (str): The path to the plain text file to be processed.
        Returns:
            data_list (list of dicts): Parsed data with normalized keys.
        Raises:
            SystemExit: If an error occurs during parsing or if the file is not readable.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read().strip()
                entries = content.split("\n\n")
                data_list = []
                for entry in entries:
                    data = {}
                    lines = entry.strip().split("\n")
                    # Extract name
                    data["name"] = lines[0].strip()
                    # Extract street
                    data["street"] = lines[1].strip()
                    # Extract city, state, and zip
                    city_state_zip = lines[-1].strip().split(",", 1)
                    data["city"] = city_state_zip[0].strip()
                    # Extract county if available
                    if len(lines) > 3:
                        data["county"] = lines[2].strip().replace("COUNTY", "").strip()
                    state_zip = city_state_zip[1].strip().split(" ", 1)
                    data["state"] = state_zip[0].strip()
                    zip_code = state_zip[1].strip()
                    if zip_code.endswith("-") and "-" not in zip_code[:-1]:
                        data["zip"] = zip_code.replace("-", "").strip()
                    else:
                        data["zip"] = zip_code.strip()
                    if data:
                        data_list.append(data)
            return data_list
        except Exception as e:
            sys.stderr.write(f"Error parsing plain text file {file_path}: {str(e)}\n")
            sys.exit(1)


def parse_arguments():
    """
    Parse command-line arguments to get a list of file paths to process.
    Returns:
        Namespace: The parsed arguments with 'files' as a list of file paths.
    """
    parser = argparse.ArgumentParser(description='Process US addresses from specified files and output JSON sorted by ZIP code.')
    parser.add_argument('files', nargs='+', help='List of file paths to process')
    return parser.parse_args()


def main():
    """
    Main execution function that processes files based on the provided command-line arguments.
    """
    args = parse_arguments()
    processor = DataProcessor()
    all_data = []  # List to accumulate all data from all file types

    # Check and process each file passed as command-line arguments
    for file_path in args.files:
        if not os.path.isfile(file_path) or not os.access(file_path, os.R_OK):
            sys.stderr.write(f'Error: File {file_path} does not exist or is not readable.\n')
            sys.exit(1)

        # Determine file type and process accordingly
        if file_path.endswith(".xml"):
            all_data.extend(processor.process_xml(file_path))
        elif file_path.endswith(".tsv"):
            all_data.extend(processor.process_tsv(file_path))
        elif file_path.endswith(".txt"):
            all_data.extend(processor.process_plain_text(file_path))
        else:
            sys.stderr.write(f"Unsupported file format: {os.path.basename(file_path)}\n")
            sys.exit(1)

    # Sort all_data by ZIP code in ascending order
    sorted_data = sorted(all_data, key=lambda x: x.get("zip", ""))

    print(json.dumps(sorted_data, indent=2))
    
if __name__ == "__main__":
    main()
