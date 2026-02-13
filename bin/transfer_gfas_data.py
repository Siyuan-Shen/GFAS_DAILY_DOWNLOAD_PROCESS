#!/usr/bin/env python3
"""=============================================================================
TRANSFER GFAS DATA
--------------------------------------------------------------------------------
============================================================================="""
import argparse
from email.message import EmailMessage
import os
import os.path
import smtplib
import sys

import paramiko as pm


# ===============================================================================
def get_script_args():
    """
    Get command line arguments and options.
    """
    description = "Upload preprocessed GFAS file to SFTP server"
    arg_parser = argparse.ArgumentParser(description=description)
    arg_parser.add_argument(
        "data_file",
        metavar="data_file",
        type=str,
        help="File to upload to SFTP server",
    )

    arg_parser.add_argument(
        "sftp_server",
        metavar="hostname",
        type=str,
        help=(
            "Hostname of the SFTP server to which GFAS data file will "
            "be uploaded"
        ),
    )

    arg_parser.add_argument(
        "sftp_directory",
        metavar="directory_path",
        type=str,
        help=(
            "Directory on the SFTP server into which the GFAS data should be "
            "placed"
        ),
    )

    arg_parser.add_argument(
        "--identity-file",
        metavar="file_path",
        type=str,
        help=(
            "Path to SSH identity file that will be used to set up "
            "connection to SFTP server"
        ),
    )

    return arg_parser.parse_args()


def check_input_file(filename):
    """
    Check state of input file before working on it.
    """
    if not (os.path.isfile(filename) and os.access(filename, os.R_OK)):
        raise IOError(f"ERROR: COULD NOT OPEN INPUT FILE {filename}!\n")


def import_key(key_path="~/.ssh/id_rsa"):
    """
    Import RSA key for authentication to webfiles.
    """
    try:
        key = pm.RSAKey.from_private_key_file(key_path)
    except IOError:
        sys.stderr.write(f"ERROR: COULD NOT OPEN KEY FILE {key_path}!\n")
        sys.exit(1)

    return key


def get_sftp_client(hostname, key):
    """
    Create SFTP client, connect, and return transport object.
    """
    host = hostname
    port = 22

    transport = pm.Transport((host, port))
    try:
        transport.connect(username="chem631", pkey=key)
    except pm.SSHException:
        sys.stderr.write("ERROR: UNABLE TO ESTABLISH SSH CONNECTION!\n")
        sys.exit(1)

    client = pm.SFTPClient.from_transport(transport)
    return client


def push_data_file(client, directory, filename):
    """
    Push preprocessed GFAS data file to webfiles.
    """
    remote_filename = os.path.join(directory, os.path.basename(filename))
    attributes = client.put(filename, remote_filename)
    return attributes


def send_notification_email(url):
    """
    Send notification email to GCST with URL of data file.
    """
    msg = EmailMessage()
    msg.set_content(
        (
            "Hi GCST,\n\n"
            "This is an automated message to let you know that last"
            " month's GFAS data is ready to be downloaded.\n\n"
            "You can find the data at:\n\n"
            f"{url}\n\n"
            "If there are any problems with this data, please let me"
            " know.\n\n"
            "Killian"
        )
    )
    msg["Subject"] = "[GFAS - new data available]"
    msg["From"] = "klcm500@york.ac.uk"
    msg["To"] = "geos-chem-support@g.harvard.edu, killian.murphy@york.ac.uk"

    smtp = smtplib.SMTP("localhost")
    smtp.send_message(msg)
    smtp.quit()


# ===============================================================================
if __name__ == "__main__":
    script_args = get_script_args()

    check_input_file(script_args.data_file)
    if script_args.identity_file is not None:
        check_input_file(script_args.identity_file)

    if script_args.identity_file is not None:
        client_key = import_key(script_args.identity_file)
    else:
        client_key = import_key()

    sftp_client = get_sftp_client(script_args.sftp_server, client_key)
    push_data_file(
        sftp_client,
        script_args.sftp_directory,
        script_args.data_file,
    )
    sftp_client.close()

    URL_PREFIX = "https://webfiles.york.ac.uk/WACL/GFAS/INCOMING/"
    url_suffix = os.path.basename(script_args.data_file)
    send_notification_email(f"{URL_PREFIX}{url_suffix}")
