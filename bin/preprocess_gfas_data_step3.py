#!/usr/bin/env python3
"""=============================================================================
Combine monthly CAMS Global Fire Assimilation System data.
--------------------------------------------------------------------------------
============================================================================="""
import argparse
import datetime
from json import load
import os
import sys

import netCDF4 as nc4
import numpy as np


def simple_mode(data):
    """
    Return the modal value from a collection of values.
    """
    unique, counts = np.unique(data, return_counts=True)
    return unique[np.argmax(counts)]


def file_path(path_string: str) -> str:
    """ """
    if os.path.isfile(path_string):
        return path_string

    _error_message = (
        f"The passed raw data file path {path_string} is not a path to an "
        "existing and accesible file"
    )
    raise argparse.ArgumentTypeError(_error_message)


def potential_file_path(path_string: str) -> str:
    """ """
    try:
        with open(path_string, "w+", encoding="utf-8") as _:
            return path_string
    except OSError as _exc:
        _error_message = (
            f"The passed output file path {path_string} is not a path to a "
            "writable location"
        )
        raise argparse.ArgumentTypeError(_error_message)


def parse_command_line() -> argparse.Namespace:
    """ """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "raw_data_file",
        type=file_path,
        metavar="/path/to/data/file",
        help="The raw GFAS data to preprocess",
    )

    parser.add_argument(
        "output_file",
        metavar="/path/to/output/file",
        type=potential_file_path,
        help="Preprocessed output file",
    )

    _variable_spec_help = (
        "JSON format variable specification, containing variable names to "
        "preprocess and their associated long names and units"
    )
    parser.add_argument(
        "--variable-spec",
        metavar="/path/to/variable/spec",
        default="variable_spec.json",
        nargs=1,
        type=file_path,
        help=_variable_spec_help,
    )

    return parser.parse_args()


def process_time_dimension(output_dataset, input_dataset):
    """
    Preprocess input_dataset time dimension, storing result in output_dataset.
    """
    output_time = output_dataset.createVariable(
        "time", np.int32, ("time",), zlib=True, chunksizes=(1,)
    )
    output_time.units = "hours since 1970-01-01 00:00:0.0"
    output_time.long_name = "time"
    output_time.calendar = "gregorian"

    time_list = np.ndarray.tolist(input_dataset.variables["time"][:])

    output_time[:] = time_list


def process_lat_dimension(output_dataset, input_dataset):
    """
    Preprocess input_dataset latitude dimension, storing result in
    output_dataset.
    """
    output_latitude = output_dataset.createVariable(
        "lat", np.float32, ("lat",), zlib=True, chunksizes=(1800,)
    )
    output_latitude.units = "degrees_north"
    output_latitude.long_name = "latitude"
    output_latitude[:] = np.ndarray.tolist(
        input_dataset.variables["latitude"][::-1]
    )


def process_lon_dimension(output_dataset, input_dataset):
    """
    Preprocess input_dataset longitude dimension, storing result in
    output_dataset.
    """
    output_longitude = output_dataset.createVariable(
        "lon", np.float32, ("lon",), zlib=True, chunksizes=(3600,)
    )
    output_longitude.units = "degrees_east"
    output_longitude.long_name = "longitude"
    output_longitude[:] = np.ndarray.tolist(
        input_dataset.variables["longitude"][:]
    )


def process_dimensions(output_dataset, input_dataset):
    """
    Preprocess time, latitude, and longitude.
    """
    process_time_dimension(output_dataset, input_dataset)
    process_lat_dimension(output_dataset, input_dataset)
    process_lon_dimension(output_dataset, input_dataset)


def process_variable(output_dataset, input_dataset, metadata):
    """
    General-purpose function for preprocessing input_dataset variables.
    """
    if metadata["code"] not in input_dataset.variables:
        sys.stderr.write(
            (
                f"WARNING: Variable {metadata['code']} specified in variable"
                "spec, but not found in input dataset.\n"
            )
        )
        return None
    sys.stderr.write(f"INFO: Processing {metadata['code']}...\n")

    miss_value = np.float32(-1.0e-31)

    output_variable = output_dataset.createVariable(
        metadata["code"],
        np.float32,
        ("time", "lat", "lon"),
        fill_value=miss_value,
        chunksizes=(1, 1800, 3600),
        zlib=True,
    )
    output_variable.units = metadata["unit"]
    output_variable.long_name = metadata["name"]
    output_variable.missing_value = miss_value

    input_data = input_dataset.variables[metadata["code"]][:, ::-1, :]

    if metadata["code"] in ["mami", "injh", "apb", "apt"]:
        input_data[input_data == simple_mode(input_data)] = miss_value
    else:
        input_data[input_data == simple_mode(input_data)] = 0.0

    output_variable[:, :, :] = input_data
    sys.stderr.write("done\n")


def process_emission_heights(output_dataset):
    """
    Additionally preprocess injection height variables.
    """
    sys.stderr.write("INFO: Processing emission heights...\n")
    miss_value = np.float32(-1.0e-31)

    for height_field in ["mami", "injh", "apb", "apt"]:
        heights = output_dataset.variables[height_field][:, :, :]
        cofire_values = output_dataset.variables["cofire"][:, :, :]

        indices = cofire_values == 0.0
        heights[indices] = miss_value

        indices = (
            (heights != miss_value)
            & (cofire_values != 0.0)
            & (heights < 1.0)
            & (heights > -1.0)
        )
        heights[indices] = 0.0

        output_dataset.variables[height_field][:, :, :] = heights
    sys.stderr.write("done\n")


def main():
    """
    Main entry point for this script.
    """
    script_args = parse_command_line()

    try:
        with open(script_args.variable_spec[0]) as variable_spec_file:
            variable_spec = load(variable_spec_file)
    except OSError as exception:
        error_message = (
            "ERROR: Unable to open variable spec file "
            f"{script_args.variable_spec[0]} for reading\n"
        )
        sys.stderr.write(error_message)
        sys.stderr.write(f"{exception.strerror}\n")
        sys.exit(1)

    try:
        input_dataset = nc4.Dataset(script_args.raw_data_file, mode="r")
        input_dataset.set_auto_mask(False)
    except OSError as exception:
        error_message = (
            "ERROR: Unable to open raw data file "
            f"{script_args.raw_data_file} for reading\n"
        )
        sys.stderr.write(error_message)
        sys.stderr.write(f"{exception.strerror}\n")
        sys.exit(1)

    try:
        output_dataset = nc4.Dataset(script_args.output_file, mode="w")
        output_dataset.set_auto_mask(False)
    except OSError as exception:
        error_message = (
            "ERROR: Unable to open output file "
            f"{script_args.output_file} for writing\n"
        )
        sys.stderr.write(error_message)
        sys.stderr.write(f"{exception.strerror}\n")
        sys.exit(1)

    date = datetime.datetime.fromtimestamp(
        (input_dataset.variables["time"][5]) * 3600
    )
    year = date.year
    month = date.month

    output_dataset.setncattr("title", f"CAMS GFAS Inventory - {year}/{month}")
    output_dataset.setncattr("conventions", "COARDS")
    output_dataset.setncattr(
        "history",
        f"Created at {str(datetime.datetime.utcnow())} by Siyuan Shen, Washington University",
    )

    output_dataset.createDimension("lon", 3600)
    output_dataset.createDimension("lat", 1800)

    time_d_size = len(input_dataset.variables["time"][:])
    output_dataset.createDimension("time", time_d_size)

    process_dimensions(output_dataset, input_dataset)

    for metadata in variable_spec["variables"]:
        process_variable(output_dataset, input_dataset, metadata)

    process_emission_heights(output_dataset)

    input_dataset.close()
    output_dataset.close()


# =========================================================================================
if __name__ == "__main__":
    main()
