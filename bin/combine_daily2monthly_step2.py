#!/usr/bin/env python3
"""=============================================================================
We need to convert daily GFAS data to monthly data.
============================================================================="""



import argparse
import netCDF4 as nc
import numpy as np
import calendar
import datetime
import logging
import os



def date_string(string: str) -> datetime.date:
    """
    Verify that a date string is in the correct format for selecting a year &
    month: YYYY-MM. Used by argparse to validate a command line argument.

    Args:
        string: A string containing a year and month, in the format YYYY-MM.
    Returns:
        A datetime.date object, representative of the passed year & month.

    Raises:
        argparse.ArgumentTypeError: The passed string couldn't be converted to a
                                    datetime.date, likely due to it being
                                    incorrectly formatted
    """
    try:
        return datetime.datetime.strptime(string, "%Y-%m").date()
    except ValueError as _exception:
        _error_message = (
            f"The passed date {string} is not valid - expected format is "
            "YYYY-MM"
        )
        raise argparse.ArgumentTypeError(_error_message) from _exception


def directory_path(path_string: str) -> str:
    """
    Verify that a path string points to a valid and accessible directory Used
    by argparse to validate a command line argument.

    Args:
        path_string: A string containing the path of a possible directory.

    Returns:
        The same string that was passed to the function, if it can be verified
        as an existing and accessible directory.

    Raises:
        argparse.ArgumentTypeError: The passed string doesn't point to a valid
                                    and accessible directory.
    """
    if os.path.isdir(path_string):
        return path_string

    _error_message = (
        f"The passed output directory path {path_string} is not a path to "
        "an existing or accessible directory"
    )
    raise argparse.ArgumentTypeError(_error_message)


# TODO: Add description and example usage
def parse_command_line() -> argparse.Namespace:
    """
    Parse command line arguments and options

    Returns:
        argparse.Namespace containing command line arguments and options, on
        successful parsing.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "month",
        type=date_string,
        metavar="month {YYYY-MM}",
        help="The month of GFAS data to retrieve",
    )

    parser.add_argument(
        "-o",
        "--output-directory",
        metavar="output_directory_path",
        default="./",
        nargs=1,
        type=directory_path,
        help="Directory in which to get daily data files",
    )

    parser.add_argument(
        "-i",
        "--input-directory",
        metavar="input_directory_path",
        default="./",
        nargs=1,
        type=directory_path,
        help="Directory in which to store monthly data files",
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    COMMAND_LINE: argparse.Namespace = parse_command_line()


    START_DATE: datetime.date = COMMAND_LINE.month
    END_DATE: datetime.date = START_DATE + datetime.timedelta(
        days=calendar.monthrange(START_DATE.year, START_DATE.month)[1] - 1
    )
    for iday, day in enumerate(range(START_DATE.day, END_DATE.day + 1)):
        current_date: datetime.date = datetime.date(
            year=START_DATE.year,
            month=START_DATE.month,
            day=day,
        )
        input_file_path: str = os.path.join(
            COMMAND_LINE.input_directory[0],
            f"GFAS_RAW_{current_date:%Y_%m_%d}.nc",
        )
        logging.info(f"Processing daily file: {input_file_path}")
        if not os.path.isfile(input_file_path):
            logging.warning(
                f"Daily file {input_file_path} does not exist - skipping"
            )
            continue
        if iday == 0:
            # First day of the month - set up the output file
            with nc.Dataset(input_file_path, "r") as input_dataset:
                output_file_path: str = os.path.join(
                    COMMAND_LINE.output_directory[0],
                    f"GFAS_RAW_{START_DATE:%Y_%m}.nc",
                )
                logging.info(f"Creating monthly output file: {output_file_path}")
                with nc.Dataset(output_file_path, "w") as output_dataset:
                    # Copy dimensions
                    total_variables_names = list(input_dataset.variables.keys())
                    # exclude valid_time, latitude, longitude
                    total_variables_names.remove('valid_time')
                    total_variables_names.remove('latitude')
                    total_variables_names.remove('longitude')
                    
                    output_dataset.createDimension('time', END_DATE.day - START_DATE.day + 1)  # Unlimited time dimension
                    
                    for name, dimension in input_dataset.dimensions.items():
                        if name != 'valid_time':
                            output_dataset.createDimension(
                                name,
                                (len(dimension) if not dimension.isunlimited() else None),
                            )
                    
                    # Copy variables
                    time_var = output_dataset.createVariable(
                        'time', np.int32, ('time',), zlib=True, chunksizes=(1,)
                    )
                    print('input_dataset.variables["valid_time"][:]', input_dataset.variables['valid_time'][:])
                    time_var[0] = input_dataset.variables['valid_time'][0] / 3600  # Convert seconds to hours
                    
                    print('time_var[0]', time_var[0])
                    print('time_var', time_var)
                    lat_var = output_dataset.createVariable(
                        'latitude', np.float32, ('latitude',), zlib=True, chunksizes=(1800,)
                    )
                    lat_var[:] = input_dataset.variables['latitude'][:]
                    
                    lon_var = output_dataset.createVariable(
                        'longitude', np.float32, ('longitude',), zlib=True, chunksizes=(3600,)
                    )
                    lon_var[:] = input_dataset.variables['longitude'][:]
                    
                    # Initialize time variable
                    for name in total_variables_names:
                        out_var = output_dataset.createVariable(
                            name,
                            input_dataset.variables[name].datatype,
                            ('time','latitude','longitude')
                        )
                        # Copy variable attributes
                        out_var.setncatts({k: input_dataset.variables[name].getncattr(k) for k in input_dataset.variables[name].ncattrs()})
                        # Initialize data array for monthly means
        # Now, add the daily data to the monthly totals
        with nc.Dataset(input_file_path, "r") as input_dataset:
            # compute time value for this day
            t_hours = input_dataset.variables['valid_time'][0] / 3600.0
            with nc.Dataset(output_file_path, "a") as output_dataset:
                if iday > 0:
                    output_dataset.variables['time'][iday] = t_hours
                    print(f"output_dataset.variables['time'][iday]", output_dataset.variables['time'][iday])
                for name in total_variables_names:
                    daily_data = input_dataset.variables[name][:]
                    output_dataset.variables[name][iday,:,:] = daily_data

    # -----------------------------
    # Save the monthly file
    # -----------------------------
    logging.info("Monthly GFAS data processing complete.")
    outdir = COMMAND_LINE.output_directory[0]
    month_tag = f"{START_DATE.year}_{str(START_DATE.month).zfill(2)}"
    logging.info(f"Monthly GFAS data saved to: {os.path.join(outdir, f'GFAS_RAW_{month_tag}.nc')}")

    