import os
import time
import shutil
import time
import pandas as pd
import numpy as np
import hashlib


def create_folder_if_not_exist(folder_path):
    """
    Create a folder at the specified folder_path if it does not already exist.

    Args:
        folder_path (str): The path of the folder to be created.

    Returns:
        None
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def scan_folder(folder):
    """
    Scan a folder, return a list of items and a list of items' paths in the folder
    """

    items = list(os.scandir(folder))
    items_path = [item.path for item in items]
    return items, items_path


def get_files_and_folders(folder_path):
    """
    Get lists of files and folders in a folder.

    Args:
        folder_path (str): The path of the folder to check.

    Returns:
        files (list): A list of file paths.
        folders (list): A list of folder paths.
    """
    files = []
    folders = []

    for entry in os.scandir(folder_path):
        if entry.is_file():
            files.append(entry)
        elif entry.is_dir():
            folders.append(entry)
    return files, folders


def do_something(new_files, folder_path):
    """
    Do something
    """
    for file in new_files:
        # Create a new txt file with the name as the time that file is copied into with the extension of txt
        print(f'*** New file detected: {file.name} ***')
        file_name = os.path.splitext(file.name)[0]
        file_extension = os.path.splitext(file.name)[1]
        new_file_name = time.strftime("%Y%m%d-%H%M%S") + ".txt"
        new_file_path = os.path.join(folder_path, new_file_name)
        with open(new_file_path, "w") as f:
            f.write(file_name)
        print(f"New file created: {new_file_name}")


def move_new_items_obsolete(folder_to_process, new_files=None, new_folders=None, check_interval=2, stable_duration=5):
    if not os.path.exists(folder_to_process):
        os.makedirs(folder_to_process)

    print('MOVE ITEM IS WORKING')

    def get_total_size(folder):
        total_size = 0
        for path, _, files in os.walk(folder):
            for f in files:
                file_path = os.path.join(path, f)
                total_size += os.path.getsize(file_path)
        return total_size

    def is_item_stable(item_path, check_interval, stable_duration, is_folder=False):
        initial_size = get_total_size(
            item_path) if is_folder else os.path.getsize(item_path)
        stable_counter = 0

        while stable_counter < stable_duration:
            time.sleep(check_interval)
            current_size = get_total_size(
                item_path) if is_folder else os.path.getsize(item_path)

            if initial_size == current_size:
                stable_counter += check_interval
            else:
                stable_counter = 0
                initial_size = current_size

    if new_files:
        for file in new_files:
            file_path = os.path.join(folder_to_monitor, file)
            dest_path = os.path.join(folder_to_process, file)
            print("-------------------------------")
            print(f"working on file: ", file_path)
            print(os.path.isfile(file_path))
            if os.path.isfile(file_path):
                is_item_stable(file_path, check_interval, stable_duration)

                # Remove the file in the folder to monitor if it exists in the folder_to_process
                if os.path.exists(dest_path):
                    print(
                        f"File already exists in {folder_to_process}, removing {file_path} ...")
                    os.remove(file_path)
                else:
                    # Copy the file to the destination folder and remove the source file
                    print(f"Copying {file_path} to {folder_to_process} ...")
                    shutil.copy2(file_path, dest_path)


def move_new_items(folder_to_monitor, folder_to_process, new_files=None, new_folders=None, check_interval=2, stable_duration=5):

    if not os.path.exists(folder_to_process):
        os.makedirs(folder_to_process)

    def get_total_size(folder):
        total_size = 0
        for path, _, files in os.walk(folder):
            for f in files:
                file_path = os.path.join(path, f)
                total_size += os.path.getsize(file_path)
        return total_size

    def is_item_stable(item_path, check_interval, stable_duration, is_folder=False):
        """
        Check if the size of an item (file or folder) remains stable for a specified duration.

        This function monitors the size of a file or folder and determines if it has stopped
        changing for a given amount of time. This can be useful for ensuring that a file or folder
        has finished copying or downloading before performing any further operations on it.

        Args:
            item_path (str): The path of the file or folder to monitor.
            check_interval (int): The time interval (in seconds) between consecutive checks of the item's size.
            stable_duration (int): The required duration (in seconds) for which the item's size must
                                remain unchanged for the function to consider it stable.
            is_folder (bool, optional): A flag indicating whether the item is a folder or not (default: False).

        Returns:
            bool: True if the item's size remains stable for the specified duration, otherwise False.
        """
        initial_size = get_total_size(
            item_path) if is_folder else os.path.getsize(item_path)
        stable_counter = 0

        while stable_counter < stable_duration:
            time.sleep(check_interval)
            current_size = get_total_size(
                item_path) if is_folder else os.path.getsize(item_path)

            if initial_size == current_size:
                stable_counter += check_interval
            else:
                stable_counter = 0
                initial_size = current_size

        return True

    def find_file_in_directory_tree(base_directory, file_name):
        for root, _, files in os.walk(base_directory):
            if file_name in files:
                return os.path.join(root, file_name)
        return None

    if new_files:
        for file_entry in new_files:
            file_path = file_entry.path
            file_name = file_entry.name
            dest_path = os.path.join(folder_to_process, file_name)

            if os.path.isfile(file_path):
                is_item_stable(file_path, check_interval, stable_duration)

                existing_file_path = find_file_in_directory_tree(
                    folder_to_process, file_name)
                if existing_file_path:
                    print(
                        f"File already exists in {folder_to_process}, removing {file_path} ...")
                    os.remove(file_path)
                else:
                    print(f"Moving {file_path} to {folder_to_process} ...")
                    shutil.move(file_path, dest_path)

    def move_items_recursively(src, dst):
        for item in os.listdir(src):
            src_item_path = os.path.join(src, item)
            dst_item_path = os.path.join(dst, item)

            if os.path.isfile(src_item_path):
                if os.path.exists(dst_item_path):
                    os.remove(src_item_path)
                else:
                    shutil.move(src_item_path, dst_item_path)
            elif os.path.isdir(src_item_path):
                if not os.path.exists(dst_item_path):
                    os.makedirs(dst_item_path)
                move_items_recursively(src_item_path, dst_item_path)

    if new_folders:
        for folder_entry in new_folders:
            folder_path = folder_entry.path
            folder_name = folder_entry.name
            dest_path = os.path.join(folder_to_process, folder_name)

            if os.path.isdir(folder_path):
                is_item_stable(folder_path, check_interval,
                               stable_duration, is_folder=True)

                print(f"Moving {folder_path} to {folder_to_process} ...")

                if os.path.exists(dest_path):
                    move_items_recursively(folder_path, dest_path)
                    os.rmdir(folder_path)
                else:
                    shutil.move(folder_path, dest_path)


def monitor_folder_obsolete(folder_to_monitor, callback_function=None):
    """This function monitors a folder and do something when there are new files in the folder

    initial_state and initial_state_path are used to keep track of the initial state of the folder, which is reflecting the existence of the files in the folder when the function is called.

    current_state and current_state_path are used to keep track of the current state of the folder, which is updating the current existence of the files in the folder every time the function is called.

    This function has an infinite loop to check the target folder regularly, and it will call the do_something() function when there are new files in the folder.

    Args:
        folder_to_monitor (string): folder to be monitored
    """

    print(f'Monitoring {folder_to_monitor} ...')

    # Get the initial state of the folder
    initial_state, initial_state_path = scan_folder(folder_to_monitor)

    while True:
        # Get the current state of the folder
        current_state, current_state_path = scan_folder(folder_to_monitor)

        # Check if there are any new files

        if len(current_state_path) != len(initial_state_path):
            new_files = [
                item for item in current_state if item.path not in initial_state_path]
        else:
            new_files = []

        if len(new_files) != 0:

            if callback_function is None:

                # Just do something with the new files
                do_something(new_files, folder_to_monitor)

                # Update the initial state of the folder
                initial_state, initial_state_path = scan_folder(
                    folder_to_monitor)

        # Wait for 7 second before checking again
        time.sleep(7)
        print(f'Monitoring {folder_to_monitor} ...')


def monitor_folder(folder_to_monitor, callback_function=None):
    print(f'Monitoring {folder_to_monitor} ...')

    # Get the initial state of the folder
    initial_state, initial_state_path = scan_folder(folder_to_monitor)
    absolute_initial_state = initial_state

    while True:
        # Get the current state of the folder
        current_state, current_state_path = scan_folder(folder_to_monitor)

        # Check if there are any new files or folders
        if len(current_state_path) != len(initial_state_path):
            new_items = [
                item for item in current_state if item.path not in initial_state_path]
            new_files = [item for item in new_items if item.is_file()]
            new_folders = [item for item in new_items if item.is_dir()]
        else:
            new_files = []
            new_folders = []

        if len(new_files) != 0 or len(new_folders) != 0:
            if callback_function is None:
                do_something(new_files, new_folders, folder_to_monitor)
            else:
                callback_function(new_files, new_folders)
            initial_state, initial_state_path = scan_folder(folder_to_monitor)

            # Update the initial state of the folder
            initial_state, initial_state_path = scan_folder(folder_to_monitor)

        if initial_state == absolute_initial_state:
            print('It is back to original state')

        # Wait for 7 seconds before checking again
        time.sleep(7)
        print(f'Monitoring {folder_to_monitor} ...')


def convert_float_string_to_float(float_string):
    """
    Convert a string representing a floating-point number to a float.

    Parameters
    ----------
    float_string : str
        The string representation of the floating-point number, which may
        contain commas as thousands separators.

    Returns
    -------
    float
        The floating-point number represented by the input string, with any
        commas removed. If the input is not a string or cannot be converted to
        a float, returns NaN.

    Raises
    ------
    None

    Examples
    --------
    convert_float_string_to_float('1,234.56')
    1234.56
    convert_float_string_to_float('1.2')
    1.2
    convert_float_string_to_float('foo')
    NaN
    convert_float_string_to_float(None)
    NaN
    """
    try:
        return float(float_string.replace(',', ''))
    except AttributeError:
        return float_string
    except ValueError:
        return np.nan


def is_not_nan(value):
    try:
        return not np.isnan(value).any()
    except TypeError:
        return True


def get_first_value(my_list):
    notna_values = [value for value in my_list if is_not_nan(value)]
    first_value = notna_values[0] if notna_values else ""
    return first_value if isinstance(first_value, str) else ""


def get_first_non_nan(input_list):
    if isinstance(input_list, np.ndarray):
        input_list = input_list.tolist()
    elif not isinstance(input_list, list):
        return ""

    for value in input_list:
        try:
            float_value = float(value)
            if not np.isnan(float_value):
                return float_value
        except (TypeError, ValueError):
            pass

    return ""


def get_last_non_nan(input_list):
    if isinstance(input_list, np.ndarray):
        input_list = input_list.tolist()
    elif not isinstance(input_list, list):
        return ""

    for value in reversed(input_list):
        try:
            float_value = float(value)
            if not np.isnan(float_value):
                return float_value
        except (TypeError, ValueError):
            pass

    return ""


def get_last_value(my_list):
    notna_values = [value for value in reversed(my_list) if is_not_nan(value)]
    last_value = notna_values[0] if notna_values else ""
    return last_value if isinstance(last_value, str) else ""


def get_sum_of_values(my_list):
    sum_of_values = 0
    for value in my_list:
        if pd.notna(value) and isinstance(value, (int, float)):
            sum_of_values += value
    return sum_of_values


def is_folder_stable(folder_path, check_interval=2, stable_duration=5):
    def get_folder_state(folder):
        folder_state = []
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                folder_state.append((file_path, os.path.getmtime(
                    file_path), os.path.getsize(file_path)))
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                folder_state.append((dir_path, os.path.getmtime(dir_path)))
        folder_state.sort()
        return folder_state

    stable_counter = 0
    initial_state = get_folder_state(folder_path)

    while stable_counter < stable_duration:
        time.sleep(check_interval)
        current_state = get_folder_state(folder_path)

        if initial_state == current_state:
            stable_counter += check_interval
        else:
            stable_counter = 0
            initial_state = current_state

    return True


if __name__ == '__main__':

    folder_to_monitor = f'C:/Users/phamsonn/Desktop/Drop_DDR_Here'
    folder_to_process = 'C:/development/Thailand_ddr_extraction/data/processing/folder_to_process'
    monitor_folder(folder_to_monitor,
                   callback_function=lambda new_files, new_folders: move_new_items(folder_to_monitor, folder_to_process, new_files, new_folders))
