# import multiprocessing
import os
import re

# import time
from pathlib import Path
import pandas as pd
import numpy as np
from PyPDF2 import PdfReader

from constants import Constants


# def check_cpu_cores():
#     # Check if the number of cores has already been saved in a configuration file
#     if os.path.exists("config.txt"):
#         with open("config.txt", "r") as file:
#             num_cores = int(file.read().strip())
#     else:
#         # If the number of cores hasn't been saved, calculate it and save it
#         if platform.system() == "Windows":
#             num_cores = os.cpu_count()
#         else:
#             num_cores = multiprocessing.cpu_count()

#         with open("config.txt", "w") as file:
#             file.write(str(num_cores))
#     return num_cores


def append_extracted_file(file_path, extracted_files=Constants.EXTRACTED_FILES):
    """Append the file name from file_path to the extracted_files.txt file

    Args:
        file_path (string): Path of the file to append
        extracted_files (string): Name of the file to append to
    """
    extracted_files_path = os.path.join(
        Constants.OUTPUT_FOLDER, extracted_files)

    file_name = os.path.basename(file_path)

    # Create the file with an empty content if it doesn't exist
    Path(extracted_files_path).touch()

    # Read the contents of the file into a string
    with open(extracted_files_path, "r+") as file:
        file_contents = file.read()
        # Check if the string is already in the file
        if file_name not in file_contents:
            # If the string is not in the file, append it
            file.write(
                file_name + "\n"
            )  # Add a newline character to separate the appended string from the previous contents
            file_contents += file_name + "\n"
        else:
            return False

    # Save and close the file
    with open(extracted_files_path, "w") as file:
        file.write(file_contents)
    return True


def generate_output_file_name(file_name):
    import datetime

    # get the current datetime object
    now = datetime.datetime.now()
    # format it as yyyymmddhhmmss
    formatted_now = now.strftime("%Y%m%d%H%M%S")
    return f"{file_name}{formatted_now}.csv"


def re_search(pattern, text):
    """Search by regex pattern and return all matches in list. If no match, return empty list

    Args:
        pattern (re.compile): pattern to search
        text (string): text to search

    Returns:
        list: list of matches
    """
    matches = re.findall(pattern, text)
    if matches:
        return matches
    return []


def all_text_in_pdf(pdf_file):
    """Extract all text from pdf_file

    Args:
        pdf_file (string): PDF file name

    Returns:
        string: all text in pdf_file
    """
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text


def list_all_pdf_files(pdf_folder):
    """Return all pdf files in the pdf_folder and its subfolders

    Args:
        pdf_folder (string): Path of pdf folder
    """
    # Get all pdf files in the pdf_folder and all of its nested subfolders
    pdf_files = list(Path(pdf_folder).rglob("*.pdf"))
    return pdf_files


pdf_folder = "DataExtractedFromDDR/data"
pdf_files = list_all_pdf_files(pdf_folder)


def convert_number_string_to_float(number_in_string):
    """
    The helper function to convert number with comma in string type
    It returns that number in float type

    Parameters
    ----------
    number_string : string
        number in string format with ',' seperator for thousands

    Returns
    -------
    number : float
        number in float format
    """
    try:
        return float(str(number_in_string).replace(",", ""))
    except:
        print(f"{number_in_string} is not float number.")


def list_of_dict_to_df(targeted_df, column_name):
    """
    The function loop through all wells in the targeted_df.
    In each well, it concat all rows with list of dictionary in the column_name
    to have a list of dictionary. It then convert that list to DataFrame

    Parameters
    ----------
    targeted_df: DataFrame
        DataFrame containing well data, including the column_name column

    column_name: string
        Name of the column to combine

    Returns
    -------
    DataFrame with first column is well name and other columns
    splitted from column_name column
    """

    result = pd.DataFrame()

    if targeted_df.empty:
        return

    wells_set = targeted_df["well"].unique()

    for well in wells_set:
        well_data = targeted_df[targeted_df["well"] == well]
        extracted_data_raw = well_data[column_name].to_list()

        extracted_data = []
        for item in extracted_data_raw:
            extracted_data += item

        extracted_data = pd.DataFrame(extracted_data).drop_duplicates()
        try:
            extracted_data = extracted_data.sort_values(by=["MD"])
        except:
            pass

        # Add column well as the first column
        extracted_data.insert(0, "well", well)

        result = pd.concat([result, extracted_data])

    return result


def combine_column(targeted_df, column_name):
    """
    Combine column values into a list for each well.

    Parameters
    ----------
    targeted_df : pandas.DataFrame
        Dataframe containing well data.
    column_name : str
        Name of the column to combine
        f"{column_name}"

    Returns
    -------
    dict_df : pandas.DataFrame
        Dataframe with combined column values as a list for each well.
    """

    # Use lazy import for pandas inside function
    import pandas as pd

    # Use set operations instead of looping through all wells
    wells_set = targeted_df["well"].unique()

    # Use list comprehension instead of appending items to a list in a loop
    dict_list = [
        {
            "well": well,
            "combined_column": targeted_df[targeted_df["well"] == well][
                column_name
            ].tolist(),
        }
        for well in wells_set
    ]

    # Convert list of dictionaries to dataframe
    dict_df = pd.DataFrame(dict_list)

    return dict_df


def search_number_ahead_marked_text(target_text, table):
    """
    The helper function to return number ahead a marked text in a string
    It returns a list of float numbers

    Parameters
    ----------
    marked_text : string
        marked_text in string which is to locate the target number
    sentence: string
        sentence in string include the marked_text

    Returns
    -------
    list of float number
    """
    pattern = re.compile(f"\d+\.?\d+\s?(?={target_text})")
    matches = [
        cell for row in table.values for cell in row if re.search(pattern, str(cell))
    ]
    if len(matches) == 0:
        return None
    return matches


def search_text_in_table(target_text, table):
    """
    The helper function to search text appear in a cell in a table
    It returns the full contents of the cell and the cell's location

    Parameters
    ----------
    target_text : string
        Representative text to locate the cell

    table : dataframe
        Dataframe of raw data

    Returns
    -------
    cell_info : string
        Contents of the located string

    row : integer
        Row index of the located cell

    column : integer
        Column index of the located cell
    """
    target_text = target_text.lower()
    where = np.where(table.applymap(
        lambda x: str(x).lower().__contains__(target_text)))
    if len(where[0]) == 0:
        pass
    else:
        row, column = where[0][0], where[1][0]
        cell_info = str(table.iloc[row, column])
        return cell_info, row, column


def update_info_to_daily_record(daily_info, daily_info_elements, daily_info_raw):
    """
    Update list of info from raw data to daily_info

    Parameters
    ----------
    daily_info_elements : list
        list of keywords for daily_info dictionary

    daily_info_raw: list
        list of info from raw data

    Returns
    -------
    None
    """
    for index, element in enumerate(daily_info_elements):
        try:
            daily_info[element] = daily_info_raw[index]
        except IndexError:
            daily_info[element] = ""


def interpolate_survey(df, well, md, incl_or_azm):
    """
    Interpolate a survey data value at a given measured depth (MD) for a given well.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe containing the survey data, with columns 'well', 'MD',
        and either 'inclination' or 'azimuth'.
    well : str
        The name of the well to interpolate for.
    md : float
        The measured depth (MD) at which to interpolate the data value.
    incl_or_azm : str
        The name of the survey data value to interpolate, either 'Incl'
        or 'Azm'.

    Returns
    -------
    float
        The interpolated survey data value at the given MD for the specified well.
        If the MD is outside the range of the dataframe, returns the string
        "MD is out of range.".

    Raises
    ------
    None
    """
    well_df = df[df['well'] == well]

    if well_df.empty or md < well_df['MD'].min() or md > well_df['MD'].max():
        return ""

    upper_df = well_df[well_df['MD'] > md]
    lower_df = well_df[well_df['MD'] < md]

    if upper_df.empty or lower_df.empty:
        return ""

    upper_row = upper_df.iloc[0]
    lower_row = lower_df.iloc[-1]

    distance_ratio = (md - lower_row['MD']) / \
        (upper_row['MD'] - lower_row['MD'])
    interpolated = lower_row[incl_or_azm] + distance_ratio * \
        (upper_row[incl_or_azm] - lower_row[incl_or_azm])

    return interpolated


def get_latest_file(target_folder, prefix_file_name):
    # Get the list of all files starting with prefix_file_name in the folder
    files = [f for f in os.listdir(
        target_folder) if f.startswith(prefix_file_name)]
    # Check if any files were found
    if not files:
        print(
            f"No files starting with '{prefix_file_name}' were found in the specified folder.")
        return None

    # Get the latest file when files are named based on the time they are created in unix time
    latest_file = max(files, key=lambda f: os.path.getctime(
        os.path.join(target_folder, f)))

    return latest_file


def get_latest_ddr_file(target_folder):
    # Get the list of all files starting with ddr in the folder
    ddr_files = [f for f in os.listdir(target_folder) if f.startswith("ddr")]
    # Check if any ddr files were found
    if not ddr_files:
        print("No files starting with 'ddr' were found in the specified folder.")
        return None

    # Get the latest ddr file when files are named based on the time they are created in unix time
    latest_ddr_file = max(ddr_files, key=lambda f: os.path.getctime(
        os.path.join(target_folder, f)))

    return latest_ddr_file


def get_latest_wells_survey_file(target_folder):
    # Get the list of all files starting with wells_survey in the folder
    wells_survey_files = [f for f in os.listdir(
        target_folder) if f.startswith("wells_survey")]

    if not wells_survey_files:
        print("No files starting with 'wells_survey' were found in the specified folder.")
        return None

    # Get the latest wells_survey file when files are named based on the time they are created in unix time
    latest_wells_survey_file = max(
        wells_survey_files, key=lambda f: os.path.getctime(os.path.join(target_folder, f)))

    return latest_wells_survey_file


def get_latest_wells_survey_file(target_folder):
    # Get the list of all files starting with wells_survey in the folder
    wells_casing_files = [f for f in os.listdir(
        target_folder) if f.startswith("wells_casing")]

    # Get the latest wells_survey file when files are named based on the time they are created in unix time
    latest_wells_casing_file = max(
        wells_casing_files, key=lambda f: os.path.getctime(os.path.join(target_folder, f)))

    return latest_wells_casing_file


def calculate_tfa(list_nozzle_info_raw):

    def tfa_of_each_nozzle_size(nozzle_count_for_each_size):
        nozzle_count, nozzle_size = map(
            int, nozzle_count_for_each_size.split('x'))
        return ((nozzle_size / 32 / 2) ** 2) * 3.14 * nozzle_count
    try:
        if not list_nozzle_info_raw or list_nozzle_info_raw == [] or list_nozzle_info_raw == np.nan:
            return "", ""

        pattern = re.compile(r"(\d+x\d+)")
        print(f'list of nozzle info raw is: {list_nozzle_info_raw}')
        list_nozzle_info = re.findall(pattern, list_nozzle_info_raw)

        result_tfa = sum(tfa_of_each_nozzle_size(nozzle_info)
                         for nozzle_info in list_nozzle_info)

        if len(list_nozzle_info) > 1:
            nozzle_info = ", ".join(list_nozzle_info)
        else:
            nozzle_info = list_nozzle_info[0]
        return nozzle_info, result_tfa
    except TypeError:
        print(f"TypeError: {list_nozzle_info_raw}")
        return "", ""


def calculate_iadc_rop(cum_depth, hours):
    if not (isinstance(cum_depth, (int, float)) and isinstance(hours, (int, float))):
        return ""

    if hours == 0:
        return ""

    iadc_rop = cum_depth / hours
    return iadc_rop
