# from GUI.GUI import GUI
# from extract_from_pdf_file.extract_from_pdf_file import *

from extract_from_pdf_folder.extract_from_pdf_folder import extract_from_pdf_folder
from consolidate_from_extracted_data.consolidate_from_extracted_data import consolidate_from_extracted_data
from constants import Constants
from helper_functions import helper_functions, misc
import os
import pandas as pd


def main_app(pdf_folder=Constants.DDR_FOLDER, temp_folder=Constants.OUTPUT_FOLDER, results_folder=Constants.OUTPUT_FOLDER_FOR_USER):

    latest_ddr_csv_file = helper_functions.get_latest_file(temp_folder, 'ddr')
    # latest_ddr_csv_file = helper_functions.get_latest_ddr_file(temp_folder)
    if latest_ddr_csv_file is None:
        latest_df_ddr = pd.DataFrame()
    else:
        latest_df_ddr = pd.read_csv(
            os.path.join(temp_folder, latest_ddr_csv_file))

    latest_wells_survey_csv_file = helper_functions.get_latest_file(
        temp_folder, 'wells_survey')
    # latest_wells_survey_csv_file = helper_functions.get_latest_wells_survey_file(
    #     temp_folder)
    if latest_wells_survey_csv_file is None:
        latest_df_wells_survey = pd.DataFrame()
    else:
        latest_df_wells_survey = pd.read_csv(os.path.join(
            temp_folder, latest_wells_survey_csv_file))

    latest_wells_casing_csv_file = helper_functions.get_latest_file(
        temp_folder, 'wells_casing')
    # latest_wells_casing_csv_file = helper_functions.get_latest_wells_casing_file(
    #     temp_folder)
    if latest_wells_casing_csv_file is None:
        latest_df_wells_casing = pd.DataFrame()
    else:
        latest_df_wells_casing = pd.read_csv(os.path.join(
            temp_folder, latest_wells_casing_csv_file))

    # Create the temp folder if it does not exist
    misc.create_folder_if_not_exist(folder_path=temp_folder)

    # Extract data from pdf files in pdf_folder
    df_ddr = extract_from_pdf_folder(pdf_folder, temp_folder)

    if df_ddr.empty:
        print("There is no new extracted info to add")
    else:
        # Convert list of dictionaries to dataframe
        wells_survey = helper_functions.list_of_dict_to_df(
            df_ddr, "survey")
        wells_casing = helper_functions.list_of_dict_to_df(
            df_ddr, "casing")

        # Save to ddr.csv file
        ddr_file = helper_functions.generate_output_file_name(
            Constants.DDR_FILE)
        latest_df_ddr = pd.concat(
            [latest_df_ddr, df_ddr], ignore_index=True)
        latest_df_ddr.to_csv(
            os.path.join(Constants.OUTPUT_FOLDER, ddr_file), index=False
        )
        print(
            f"Saved DDR info to {ddr_file} file in {Constants.OUTPUT_FOLDER} folder",
        )

        # Save to wells_survey.csv file
        wells_survey_file = helper_functions.generate_output_file_name(
            Constants.WELLS_SURVEY_FILE)
        latest_df_wells_survey = pd.concat(
            [latest_df_wells_survey, wells_survey], ignore_index=True)
        latest_df_wells_survey.to_csv(
            os.path.join(Constants.OUTPUT_FOLDER, wells_survey_file),
            index=False,
        )
        print(
            f"Saved well surveys to {wells_survey_file} file in {Constants.OUTPUT_FOLDER} folder",
        )

        # Save to wells_casing.csv file
        wells_casing_file = helper_functions.generate_output_file_name(
            Constants.WELLS_CASING_FILE)
        latest_df_wells_casing = pd.concat(
            [latest_df_wells_casing, wells_casing], ignore_index=True)
        latest_df_wells_casing.to_csv(
            os.path.join(Constants.OUTPUT_FOLDER, wells_casing_file),
            index=False,
        )
        print(
            f"Saved well casing info to {wells_casing_file} in {Constants.OUTPUT_FOLDER} folder",
        )

    # Consolidate the extracted data of lastest extractiono of pdf files from ddr file and wells_survey file in target_folder. The compiled data will be saved to results_folder as the results.csv file
    df_result_to_user = consolidate_from_extracted_data(
        latest_df_ddr, latest_df_wells_survey)
    # df_result_to_user = consolidate_from_extracted_data(
    #     target_folder=Constants.OUTPUT_FOLDER, results_folder=results_folder)

    misc.create_folder_if_not_exist(folder_path=results_folder)
    results_file = os.path.join(results_folder, 'results.csv')

    df_result_to_user.to_csv(results_file, index=False)
    print(f"SAVED TO FINAL CSV {results_file}")


if __name__ == "__main__":
    # gui = GUI()
    # pass
    # pdf_file = "data/for_testing/001#DDR no.1_BK-23-H_R_19-Nov-2022.pdf"
    # pdf_file = "data/for_testing/041#RIGT18-cm_DDR_no.5_BK-23-L_R_8-Dec-2022_Service.pdf"
    # df, _ = extract_from_pdf_file(pdf_file)
    # print(df)
    main_app(pdf_folder=Constants.DDR_FOLDER,
             temp_folder=Constants.OUTPUT_FOLDER,
             results_folder=Constants.OUTPUT_FOLDER_FOR_USER)
