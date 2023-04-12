from helper_functions.helper_functions import *
import tabula
from PyPDF2 import PdfReader
import re

use_test_value = False
test_value = None


def extract_from_pdf_file(pdf_file):
    """
    Long function using above helper functions to extract data
    from a pdf file and return a dataframe

    Parameters
    ----------
    pdf_file : string
        name of the pdf file

    Returns
    -------
    A dataframe containing data from the pdf_file
    """

    ########################################

    daily_info = {}
    daily_info["file_name"] = os.path.basename(pdf_file)

    tables = tabula.read_pdf(pdf_file, pages="all", multiple_tables=True)

    ######################################
    # Extract all texts from pdf_file for day and TVD info
    text = all_text_in_pdf(pdf_file)

    # Find days (dd-Mmm-yyyy) in the report to find the report day
    # Find all days, then assigned the first data to the day column
    # in the resulted dataframe. If cannot find any days from re,
    # we use the less robust way
    pattern = re.compile("\d{1,2}-\w{3}-\d{4}")
    # matches = re.finditer(pattern, text)
    # matches_day = [match.group() for match in matches]
    matches_day = re_search(pattern, text)

    try:
        daily_info["day"] = matches_day[0]
    except IndexError:
        daily_info["day"] = tables[0].columns[0].split("DAILY")[0].strip()

    # Find the TD TVD in the texts by searching TVD in the
    # middle of 'Operation Summary' and 'Planned Operation'
    pattern = re.compile(
        "Operation Summary[\s\S]*?(\d+?\.?\d*)\s?mTVD[\s\S]*?Planned Operation"
    )
    # matches = re.findall(pattern, text)
    matches = re_search(pattern, text)

    try:
        daily_info["TVD_max"] = convert_number_string_to_float(max(matches))
    except ValueError:
        daily_info["TVD_max"] = ""
    except TypeError:
        daily_info["TVD_max"] = ""

    # Find rotate and slide
    steerings = ['rotate', 'slide']
    for steering in steerings:
        pattern = re.compile(
            f"{steering}:[\s\S](\d+\.?\d*)[\s\S]?m/[\s\S](\d+\.?\d*)[\s\S]*?hrs")
        matches = re_search(pattern, text.lower())
        try:
            daily_info[f"{steering}_distance"] = float(matches[0][0])
            daily_info[f"{steering}_hrs"] = float(matches[0][1])
        except IndexError:
            daily_info[f"{steering}_distance"] = ""
            daily_info[f"{steering}_hrs"] = ""

    ######################################
    # Get well and rig info
    target_text = "Rig"
    table = tables[0]

    _, row, column = search_text_in_table(target_text, table)

    ######################################
    # well_rig_info has the same column but 1 more row from the cell with target_text

    try:
        well = text.splitlines()[2].strip()
        platform = well.split("-")[0]

        daily_info["well"] = well
        daily_info["platform"] = platform
        daily_info["rig"] = tables[0].iloc[row + 1, column].split()[-1].strip()
    except:
        daily_info["rig"] = ""

    # if use_test_value:
    #     print("Check well, platform and rig")
    #     return (text, daily_info["well"], daily_info["platform"], daily_info["rig"])

    ######################################
    # Size of bit (Just to ensure)
    try:
        daily_info["size"] = tables[0].iloc[3, 1].split('" x ')[0].strip()
    except AttributeError:
        daily_info["size"] = ""

    ######################################
    # Get bit info
    target_text = "Bit and Core Head"
    table = tables[1]

    try:
        _, row, column = search_text_in_table(target_text, table)

        # bit_info has the same column but 1 more row from the cell with target_text
        try:
            bit_info = table.iloc[row + 1, column].split(",")
            bit_info = [el.strip() for el in bit_info]
        except AttributeError:
            bit_info = []
    except TypeError:
        bit_info = []

    daily_info_bit = ["bit_size", "bit_mfg", "bit_type", "bit_sn"]
    update_info_to_daily_record(daily_info, daily_info_bit, bit_info)

    ######################################
    # Get bit run info
    target_text = "bit run"
    table = tables[1]

    try:
        _, row, column = search_text_in_table(target_text, table)

        try:
            bit_run_info = table.iloc[row + 1, column].split(" ")

            incorrect_bit_run_info = True
            for info in bit_run_info:
                if re.search(r"\d", info):
                    incorrect_bit_run_info = False
                    break
            if incorrect_bit_run_info:
                raise ValueError(f"Bit run info '{bit_run_info}'is incorrect")
        except AttributeError:
            bit_run_info = []
    except:
        bit_run_info = []

    daily_info_bit_run_elements = [
        "bit_run_number",
        "bit_run_daily_start",
        "bit_run_daily_end",
    ]

    update_info_to_daily_record(
        daily_info, daily_info_bit_run_elements, bit_run_info)

    # if use_test_value:
    #     print("Check bit runs")
    #     return (
    #         daily_info["bit_run_number"],
    #         daily_info["bit_run_daily_start"],
    #         daily_info["bit_run_daily_end"],
    #     )

    ######################################
    target_text = "Cum Depth (m)"
    table = tables[1]

    try:
        _, row, column = search_text_in_table(target_text, table)
        daily_info["cum_depth"] = convert_number_string_to_float(
            table.iloc[row + 1, column]
        )
    except TypeError:
        daily_info["cum_depth"] = ""

    try:
        target_text = "Cum Time (hr)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        daily_info["cum_time"] = convert_number_string_to_float(
            table.iloc[row + 1, column]
        )
    except TypeError:
        daily_info["cum_time"] = ""

    ######################################
    target_text = "Bit Dull"
    table = tables[1]
    try:
        _, row, column = search_text_in_table(target_text, table)

        text = table.iloc[row + 1, column]
        pattern = re.compile("\d-\d-.+?-\w+-\w-\w+-.+?-\w+?")
        matches = re.findall(pattern, text)

        daily_info["dull"] = matches[0]
    except:
        daily_info["dull"] = ""

    ######################################
    target_text = "Nozzle (32nd"
    table = tables[1]

    try:
        _, row, column = search_text_in_table(target_text, table)
        nozzle_raw = table.iloc[row + 1, column].split(" ")
        nozzle_raw = [nozzle for nozzle in nozzle_raw if "x" in nozzle]

        daily_info["nozzles"] = nozzle_raw
    except:
        daily_info["nozzles"] = []

    ######################################
    try:
        target_text = "WOB (kip)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        wob_raw = table.iloc[row + 1, column].split(" ")[-1]

        daily_info["wob"] = wob_raw
    except:
        daily_info["wob"] = ""

    ######################################

    try:
        target_text = "RPM (rpm)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        rpm_raw = table.iloc[row + 1, column].split(" ")[0]

        daily_info["rpm"] = rpm_raw
    except:
        daily_info["rpm"] = ""

    ######################################
    try:
        target_text = "ROP (ft/hr)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        rop_raw = table.iloc[row + 1, column].split(" ")[-1]

        daily_info["rop"] = rop_raw
    except TypeError:
        daily_info["rop"] = ""

    ######################################
    try:
        target_text = "Flow (L/min)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        flow_raw = table.iloc[row + 1, column].split(" ")[-1]

        daily_info["flow"] = flow_raw
    except:
        daily_info["flow"] = ""

    ######################################
    try:
        target_text = "SPP (psi)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        spp_raw = table.iloc[row + 1, column]

        daily_info["spp"] = spp_raw
    except:
        daily_info["spp"] = ""

    ######################################
    try:
        target_text = "On Btm (ft-lbf)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        tq_raw = table.iloc[row + 1, column]

        daily_info["tq"] = tq_raw
    except:
        daily_info["tq"] = ""

    ######################################
    try:
        target_text = "Mud Weight (sg)"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        mw_raw = table.iloc[row + 1, column]

        daily_info["mw"] = mw_raw
    except:
        daily_info["mw"] = ""

    ######################################
    try:
        target_text = "Mud Type"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)
        mud_type_raw = table.iloc[row + 1, column]

        daily_info["mud_type"] = mud_type_raw
    except:
        daily_info["mud_type"] = ""

    ######################################
    # Extract BHA information
    try:
        target_text = "BHA Run"
        table = tables[1]
        _, row, column = search_text_in_table(target_text, table)

        daily_info["BHA"] = table.iloc[row + 1, column]

        incorrect_bha_info = True
        if re.search(r"\d", daily_info["BHA"]):
            incorrect_bha_info = False
        if incorrect_bit_run_info:
            raise ValueError("BHA info is incorrect.")

        bha = daily_info["BHA"]

        if "ags" in daily_info["BHA"].lower():
            daily_info["drive sys"] = "AGS"

            # Search Near Bit Stabilizers and their OD
            #
            pattern = re.compile(
                ',\s(?P<NB_size>\d.*?)?"?\s?(?P<NB_type>\w*?NB)')
            matches = re.finditer(pattern, daily_info["BHA"])
            NBs = [match.groupdict() for match in matches]
            try:
                daily_info["AGS-BHA"] = f'{NBs[0]["NB_type"]}+{len(NBs)-1}'
                daily_info["Under Gauge MNB/SMNB"] = NBs[0]["NB_size"]
            except:
                daily_info["AGS-BHA"] = ""
        else:
            daily_info["drive sys"] = "Motor"
            daily_info["AGS-BHA"] = "Motor"
            daily_info["Under Gauge MNB/SMNB"] = ""
    except:
        daily_info["BHA"] = ""
        daily_info["drive sys"] = ""
        daily_info["AGS-BHA"] = ""
        daily_info["Under Gauge MNB/SMNB"] = ""

    if use_test_value:
        print("Check BHA")
        return (
            daily_info["BHA"],
            daily_info["drive sys"],
            daily_info["AGS-BHA"],
            daily_info["Under Gauge MNB/SMNB"],
        )

    ######################################
    # Get casing info
    try:
        target_text = "OD (in)"
        table = tables[0]

        _, row, column = search_text_in_table(target_text, table)

        casing_info = tables[0].iloc[row + 1, column].split(" ")
        if len(casing_info) < 3:
            casing_depths = tables[0].iloc[row + 1, column + 1].split(" ")
            for depth in casing_depths:
                casing_info.append(depth)

        casing_list = [convert_number_string_to_float(
            info) for info in casing_info]
        casing_para_list = ["OD", "MD", "TVD"]

        casing_info_dict = []
        casing_info_dict.append({})
        for index, para in enumerate(casing_para_list):
            casing_info_dict[0][para] = convert_number_string_to_float(
                casing_info[index]
            )

        daily_info["casing"] = casing_info_dict
    except:
        daily_info["casing"] = {}

    # Get survey info
    try:
        target_text = "Survey Data"
        table = tables[1]

        _, row, column = search_text_in_table(target_text, table)

        count = 2
        survey_info = []
        while True:
            try:
                survey_info_depth = max(
                    tables[1].iloc[row + count, column].split())
                survey_info_inclination = (
                    tables[1].iloc[row + count, column + 1].split()[0]
                )
                survey_info_azimuth = tables[1].iloc[row +
                                                     count, column + 1].split()[1]
                survey_info.append(
                    {
                        "MD": convert_number_string_to_float(survey_info_depth),
                        "Incl": convert_number_string_to_float(survey_info_inclination),
                        "Azm": convert_number_string_to_float(survey_info_azimuth),
                    }
                )
                count += 1
            except:
                break

        daily_info["survey"] = survey_info
    except:
        daily_info["survey"] = []

    return daily_info, tables[1]
