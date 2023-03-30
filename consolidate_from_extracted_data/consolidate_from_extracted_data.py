import math
import os
import pandas as pd
import numpy as np
from constants import Constants
from helper_functions import helper_functions
from helper_functions import misc


def consolidate_from_extracted_data(latest_df_ddr, latest_df_wells_survey):

    result = []

    for (well, bit_sn, bit_run_number), group in latest_df_ddr.groupby(['well', 'bit_sn', 'bit_run_number'], as_index=False):

        date_in = misc.get_first_value(group.day.values) if (group.day.any(
        ) & group.bit_run_daily_start.any() & group.bit_sn.any()) else np.nan

        td_date = misc.get_first_value(group.day.values) if (
            group.day.any() & group.dull.any()) else np.nan

        rig = misc.get_first_value(
            group.rig.values) if group.rig.any() else np.nan

        platform = misc.get_first_value(
            group.platform.values) if group.platform.any() else np.nan

        size = misc.get_first_value(group.bit_size.values).split(" ")[
            0] if group.bit_size.any() else np.nan

        bit_type = misc.get_first_value(
            group.bit_type.values) if group.bit_type.any() else np.nan

        bit_condition = misc.get_first_value(
            group.bit_run_number.values) if group.bit_run_number.any() else np.nan

        bit_condition = 'New' if 'new' in group.bit_run_number.str.lower().values else 'RR'

        mfg = misc.get_first_value(
            group.bit_mfg.values) if group.bit_mfg.any() else np.nan

        drive_sys = misc.get_first_value(
            group['drive sys'].values) if group['drive sys'].any() else np.nan

        under_gauge = misc.get_first_value(
            group['Under Gauge MNB/SMNB'].values) if group['Under Gauge MNB/SMNB'].any() else np.nan

        md_in = misc.convert_float_string_to_float(misc.get_first_value(
            group.bit_run_daily_start.values)) if group.bit_run_daily_start.any() else np.nan

        md_out = misc.convert_float_string_to_float(misc.get_last_value(
            group.bit_run_daily_end.values)) if group.bit_run_daily_end.any() else np.nan

        cum_depth = misc.get_last_non_nan(
            group.cum_depth.values) if group.cum_depth.any() else np.nan

        # Checking step as sometimes either md_in, md_out or cum_depth is incorrect
        try:
            if (not math.isnan(md_out) and bool(md_out) and not math.isnan(cum_depth) and bool(cum_depth)):
                md_in_updated = md_out - cum_depth

                if md_in_updated > 0:
                    md_in = md_in_updated
                else:
                    md_out = md_in + cum_depth
        except:
            print(f'Error in depth: {md_in}, {md_out}, {cum_depth}')

        hours = misc.get_last_non_nan(
            group.cum_time.values) if group.cum_time.any() else np.nan

        iadc_rop = helper_functions.calculate_iadc_rop(cum_depth, hours)

        rotate_distance = misc.get_sum_of_values(
            group.rotate_distance.values) if group.rotate_distance.any() else np.nan

        rotate_hrs = misc.get_sum_of_values(
            group.rotate_hrs.values) if group.rotate_hrs.any() else np.nan

        slide_distance = misc.get_sum_of_values(
            group.slide_distance.values) if group.slide_distance.any() else np.nan

        slide_hrs = misc.get_sum_of_values(
            group.slide_hrs.values) if group.slide_hrs.any() else np.nan

        incl_in = helper_functions.interpolate_survey(
            latest_df_wells_survey, well, md_in, 'Incl')
        incl_out = helper_functions.interpolate_survey(
            latest_df_wells_survey, well, md_out, 'Incl')

        azm_in = helper_functions.interpolate_survey(
            latest_df_wells_survey, well, md_in, 'Azm')
        azm_out = helper_functions.interpolate_survey(
            latest_df_wells_survey, well, md_out, 'Azm')

        dull = misc.get_first_value(
            group.dull.values) if group.dull.any() else np.nan

        bha_description = misc.get_first_value(
            group.BHA.values) if group.BHA.any() else np.nan

        nozzles = misc.get_first_value(group.nozzles.values).replace(
            "['", "").replace("']", "") if group.nozzles.any() else np.nan

        nozzles, tfa = helper_functions.calculate_tfa(nozzles)

        result.append({
            'date_in': date_in,
            'td_date': td_date,
            'rig': rig,
            'platform': platform,
            'well': well,
            'size': size,
            'type': bit_type,
            'bit_sn': bit_sn,
            'bit_condition': bit_condition,
            'run_number': bit_run_number,
            'mfg': mfg,
            'drive_sys': drive_sys,
            'under_gauge_MNB_SMNB': under_gauge,
            'md_in': md_in,
            'md_out': md_out,
            'distance': cum_depth,
            'hours': hours,
            'iadc_rop': iadc_rop,
            'rotate_distance': rotate_distance,
            'rotate_hrs': rotate_hrs,
            'slide_distance': slide_distance,
            'slide_hrs': slide_hrs,
            'incl_in': incl_in,
            'incl_out': incl_out,
            'azm_in': azm_in,
            'azm_out': azm_out,
            'dull': dull,
            'bha_description': bha_description,
            'nozzles': nozzles,
            'tfa': tfa
        })

    # Create the result dataframe from the list of dictionaries
    result = pd.DataFrame.from_records(result, index=None)

    return result
