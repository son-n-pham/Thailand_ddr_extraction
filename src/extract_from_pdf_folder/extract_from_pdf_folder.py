import pandas as pd
from pathlib import Path
# import tkinter as tk


from extract_from_pdf_file.extract_from_pdf_file import *
from helper_functions import helper_functions
from helper_functions import misc

from constants import Constants

FILE_SEPERATOR = "====================================================\n"
COMPLETION_SEPERATOR = "\n**************************\n"


def extract_from_pdf_folder(pdf_folder, temp_folder):
    """Extract data from pdf files in a folder. The function also creates a text file appending the list of pdf files that the function has extracted from. The function will not extract data from pdf files that are already in the text file.

    Args:
        pdf_folder (string): folder containing pdf files or subfolders with pdf files
        temp_folder (string): temporary folder to store extracted files

    Returns:
        pd.DataFrame: dataframe containing extracted data from pdf files
    """
    pdf_files = helper_functions.list_all_pdf_files(pdf_folder)
    number_of_pdf_files = len(pdf_files)
    print(f"Total number of pdf files: {number_of_pdf_files}")

    result = []

    file_path = os.path.join(temp_folder, Constants.EXTRACTED_FILES)
    # Create the file if it does not exist
    if not os.path.exists(file_path):
        with open(file_path, 'w'):
            pass  # This creates an empty file

    # Open extracted_files.txt to check if the file is already extracted
    extracted_files_path = os.path.join(
        temp_folder, Constants.EXTRACTED_FILES
    )
    Path(extracted_files_path).touch()

    # Read the contents of the file extracted_files into a string
    with open(extracted_files_path, "r+") as file:
        file_contents = file.read()

    # Loop through all the pdf files
    # Extract pdf files if they are not already extracted
    for index, pdf_file in enumerate(pdf_files):

        file_name = os.path.basename(pdf_file)

        if file_name not in file_contents:
            print(FILE_SEPERATOR)
            file_contents += file_name + "\n"

            print(
                f"{index+1} of {number_of_pdf_files} files. Extracting from {file_name}\n",
            )

            well_data, _ = extract_from_pdf_file(pdf_file)
            result.append(well_data)

        # else:
        #     print(
        #         f"{index+1} of {number_of_pdf_files} files. {file_name} was already extracted\n",
        #     )

    # Save and close the file
    with open(extracted_files_path, "w") as file:
        file.write(file_contents)

    print(COMPLETION_SEPERATOR)
    print("Extraction is completed :)")
    print(COMPLETION_SEPERATOR)

    return pd.DataFrame.from_records(result)
