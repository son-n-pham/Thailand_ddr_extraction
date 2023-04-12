# from GUI.GUI import GUI
# from extract_from_pdf_file.extract_from_pdf_file import *

from extract_from_pdf_folder.extract_from_pdf_folder import extract_from_pdf_folder
from consolidate_from_extracted_data.consolidate_from_extracted_data import consolidate_from_extracted_data
from constants import Constants
from helper_functions import helper_functions, misc
import os
import shutil
import time
import pandas as pd


def main_extraction_from_folder(pdf_folder=Constants.PROCESSING_FOLDER, temp_folder=Constants.TEMP_FOLDER, output_folder=Constants.OUTPUT_FOLDER):

    latest_ddr_csv_file = helper_functions.get_latest_file(temp_folder, 'ddr')
    if latest_ddr_csv_file is None:
        latest_df_ddr = pd.DataFrame()
    else:
        latest_df_ddr = pd.read_csv(
            os.path.join(temp_folder, latest_ddr_csv_file))

    latest_wells_survey_csv_file = helper_functions.get_latest_file(
        temp_folder, 'wells_survey')

    if latest_wells_survey_csv_file is None:
        latest_df_wells_survey = pd.DataFrame()
    else:
        latest_df_wells_survey = pd.read_csv(os.path.join(
            temp_folder, latest_wells_survey_csv_file))

    latest_wells_casing_csv_file = helper_functions.get_latest_file(
        temp_folder, 'wells_casing')

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
            os.path.join(temp_folder, ddr_file), index=False
        )
        print(
            f"Saved DDR info to {ddr_file} file in {temp_folder} folder",
        )

        # Save to wells_survey.csv file
        wells_survey_file = helper_functions.generate_output_file_name(
            Constants.WELLS_SURVEY_FILE)
        latest_df_wells_survey = pd.concat(
            [latest_df_wells_survey, wells_survey], ignore_index=True)
        latest_df_wells_survey.to_csv(
            os.path.join(temp_folder, wells_survey_file),
            index=False,
        )
        print(
            f"Saved well surveys to {wells_survey_file} file in {temp_folder} folder",
        )

        # Save to wells_casing.csv file
        wells_casing_file = helper_functions.generate_output_file_name(
            Constants.WELLS_CASING_FILE)
        latest_df_wells_casing = pd.concat(
            [latest_df_wells_casing, wells_casing], ignore_index=True)
        latest_df_wells_casing.to_csv(
            os.path.join(temp_folder, wells_casing_file),
            index=False,
        )
        print(
            f"Saved well casing info to {wells_casing_file} in {temp_folder} folder",
        )

    # Consolidate the extracted data of latest extraction of pdf files from ddr file and wells_survey file in target_folder. The compiled data will be saved to output_folder as the results.csv file
    df_result_to_user = consolidate_from_extracted_data(
        latest_df_ddr, latest_df_wells_survey)

    misc.create_folder_if_not_exist(folder_path=output_folder)
    results_file = os.path.join(output_folder, 'results.csv')

    try:
        df_result_existing = pd.read_csv(results_file)
    except FileNotFoundError:
        print("No existing results.csv file")

    try:
        df_result_to_user = misc.append_dataframes(
            df_result_to_user, df_result_existing)
        df_result_to_user.to_csv(results_file, index=False)
        print(f"SAVED TO FINAL CSV {results_file}")
    except AttributeError:
        print("No new data to save to final csv")
    except NameError:
        df_result_to_user.to_csv(results_file, index=False)

    misc.create_folder_if_not_exist(
        folder_path=Constants.OUTPUT_FOLDER_DESKTOP)

    try:
        shutil.copy(results_file, Constants.OUTPUT_FOLDER_DESKTOP)
    except FileNotFoundError:
        print(
            "No results.csv file to copy to {Constants.OUTPUT_FOLDER_DESKTOP}")
    except PermissionError:
        print('Permission denied to copy results.csv to desktop/output folder')


def monitor_folder(folder_to_monitor):

    print(f'Monitoring {folder_to_monitor} ...')

    # # Get the initial state of the folder
    # initial_state, initial_state_path = misc.scan_folder(folder_to_monitor)
    # absolute_initial_state = initial_state

    while True:
        misc.create_folder_if_not_exist(folder_path=folder_to_monitor)

        files, folders = misc.get_files_and_folders(
            folder_path=folder_to_monitor)

        if files or folders:
            misc.move_new_items(folder_to_monitor,
                                folder_to_process,
                                files,
                                folders)
        else:
            misc.create_folder_if_not_exist(
                folder_path=Constants.PROCESSING_FOLDER)

            processing_files, processing_folders = misc.get_files_and_folders(
                folder_path=Constants.PROCESSING_FOLDER)

            if processing_files or processing_folders:
                main_extraction_from_folder(
                    pdf_folder=Constants.PROCESSING_FOLDER, temp_folder=Constants.TEMP_FOLDER,
                    output_folder=Constants.OUTPUT_FOLDER)

                misc.move_new_items(Constants.PROCESSING_FOLDER,
                                    Constants.ARCHIVE_FOLDER, processing_files, processing_folders)

        # # Get the current state of the folder
        # current_state, current_state_path = misc.scan_folder(folder_to_monitor)

        # # Check if there are any new files or folders
        # if len(current_state_path) != len(initial_state_path):
        #     new_items = [
        #         item for item in current_state if item.path not in initial_state_path]
        #     new_files = [item for item in new_items if item.is_file()]
        #     new_folders = [item for item in new_items if item.is_dir()]

        #     # is_new_items_in_drop_folder = True
        # else:
        #     new_files = []
        #     new_folders = []

        # if len(new_files) != 0 or len(new_folders) != 0:
        #     if callback_function is None:
        #         print('No callback function')
        #         # do_something(new_files, new_folders, folder_to_monitor)
        #     else:
        #         callback_function(new_files, new_folders)

            # # Update the initial state of the folder
            # initial_state, initial_state_path = misc.scan_folder(
            #     folder_to_monitor)

        # if is_folder_empty(pdf_folder):
        #     return

        # if initial_state == absolute_initial_state:
        #     print('It is back to original state')

        #     if len(os.listdir(Constants.PROCESSING_FOLDER)) != 0:
        #         print('run main_extraction_from_folder')
        #         main_extraction_from_folder(pdf_folder=Constants.PROCESSING_FOLDER,
        #                                     temp_folder=Constants.TEMP_FOLDER,
        #                                     output_folder=Constants.OUTPUT_FOLDER)

        #         processing_files, processing_folders = misc.get_files_and_folders(
        #             folder_path=Constants.PROCESSING_FOLDER)

        #         misc.move_new_items(Constants.PROCESSING_FOLDER,
        #                             Constants.ARCHIVE_FOLDER, processing_files, processing_folders)
        #         print('after move_new_items')

        # Wait for 1 seconds before checking again
        time.sleep(1)
        print(f'Monitoring {folder_to_monitor} ...')


if __name__ == "__main__":
    # gui = GUI()
    # pass
    # pdf_file = "data/for_testing/001#DDR no.1_BK-23-H_R_19-Nov-2022.pdf"
    # pdf_file = "data/for_testing/041#RIGT18-cm_DDR_no.5_BK-23-L_R_8-Dec-2022_Service.pdf"
    # df, _ = extract_from_pdf_file(pdf_file)
    # print(df)

    folder_to_monitor = f'C:/Users/{Constants.USERNAME}/Desktop/Drop_DDR_Here'
    folder_to_process = Constants.PROCESSING_FOLDER

    monitor_folder(folder_to_monitor)
    # monitor_folder(folder_to_monitor, callback_function=lambda new_files,
    #                new_folders: misc.move_new_items(folder_to_monitor, folder_to_process, new_files, new_folders))

    # main_extraction_from_folder(pdf_folder=Constants.DDR_FOLDER,
    #                             temp_folder=Constants.OUTPUT_FOLDER,
    #                             output_folder=Constants.OUTPUT_FOLDER_FOR_USER)

    # processing_files, processing_folders = misc.get_files_and_folders(
    #     folder_path=Constants.PROCESSING_FOLDER)

    # misc.move_new_items(Constants.PROCESSING_FOLDER,
    #                     Constants.ARCHIVE_FOLDER, processing_files, processing_folders)
