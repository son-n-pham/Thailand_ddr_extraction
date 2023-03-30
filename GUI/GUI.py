import tkinter as tk
import tkinter.filedialog as filedialog

import os
from extract_from_pdf_folder.extract_from_pdf_folder import extract_from_pdf_folder
from consolidate_from_extracted_data.consolidate_from_extracted_data import consolidate_from_extracted_data


from constants import Constants
from helper_functions.helper_functions import *


class GUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Data Extraction for PTTEP")
        self.setup_select_button()
        self.setup_console()
        self.root.mainloop()

    def setup_select_button(self):
        self.select_button = tk.Button(
            self.root, text="Select PDF Folder", command=self.select_folder
        )
        self.select_button.pack()

    def setup_console(self):
        self.console_frame = tk.Frame(self.root)
        self.console_frame.pack()
        self.console = tk.Text(self.console_frame, wrap="word")
        self.vscrollbar = tk.Scrollbar(
            self.console_frame, orient="vertical", command=self.console.yview
        )
        self.vscrollbar.pack(side="right", fill="y")
        self.hscrollbar = tk.Scrollbar(
            self.console_frame, orient="horizontal", command=self.console.xview
        )
        self.hscrollbar.pack(side="bottom", fill="x")
        self.console.config(
            xscrollcommand=self.hscrollbar.set, yscrollcommand=self.vscrollbar.set
        )
        self.console.pack()

    def main_app(self, pdf_folder, console, root):
        ddr_info = extract_from_pdf_folder(pdf_folder, console, root)

        try:
            wells_survey = list_of_dict_to_df(ddr_info, "survey")
            wells_casing = list_of_dict_to_df(ddr_info, "casing")

            # check if the directory exists, if not, create it
            if not os.path.exists(Constants.OUTPUT_FOLDER):
                os.makedirs(Constants.OUTPUT_FOLDER)
                
            if not os.path.exists(Constants.OUTPUT_FOLDER_FOR_USER):
                os.makedirs(Constants.OUTPUT_FOLDER_FOR_USER)

            # Save to ddr.csv file
            ddr_file = generate_output_file_name(Constants.DDR_FILE)
            ddr_info.to_csv(
                os.path.join(Constants.OUTPUT_FOLDER, ddr_file), index=False
            )
            console.insert(
                tk.END,
                f"Saved DDR info to {ddr_file} file in {Constants.OUTPUT_FOLDER} folder\n",
            )
            console.see("end")

            # Save to wells_survey.csv file
            wells_survey_file = generate_output_file_name(Constants.WELLS_SURVEY_FILE)
            wells_survey.to_csv(
                os.path.join(Constants.OUTPUT_FOLDER, wells_survey_file),
                index=False,
            )
            console.insert(
                tk.END,
                f"Saved well surveys to {wells_survey_file} file in {Constants.OUTPUT_FOLDER} folder\n",
            )
            console.see("end")

            # Save to wells_casing.csv file
            wells_casing_file = generate_output_file_name(Constants.WELLS_CASING_FILE)
            wells_casing.to_csv(
                os.path.join(Constants.OUTPUT_FOLDER, wells_casing_file),
                index=False,
            )
            console.insert(
                tk.END,
                f"Saved well casing info to {wells_casing_file} in {Constants.OUTPUT_FOLDER}\n",
            )
            console.see("end")
        except:
            print("There is no new extracted info to add")

    def select_folder(self):
        folder_path = filedialog.askdirectory()
        self.console.insert(
            tk.END,
            f"{folder_path} is selected.\nStart extracting all PDF files in this folder.\n\n",
        )
        self.console.see("end")
        self.root.update()
        self.select_button["state"] = "disable"

        self.main_app(folder_path, self.console, self.root)

        self.select_button["state"] = "normal"
