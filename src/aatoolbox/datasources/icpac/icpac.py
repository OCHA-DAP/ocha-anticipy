"""Retrieve data from ICPAC's FTP server."""
import os
from ftplib import FTP, FTP_TLS

# TODO: decide we would like to use dotenv or not
from dotenv import load_dotenv


def connect_icpac_ftp(
    ftp_address: str,
    ftp_username: str,
    ftp_password: str,
) -> FTP_TLS:
    """
    Connect to ICPAC's ftp.

    To connect you need credentials.

    Parameters
    ----------
    ftp_address : str
        IP address of the ftp server
    ftp_username : str
        username of the ftp server
    ftp_password : str
        password of the ftp server

    Returns
    -------
    ftps: FTP
        a FTP object
    """
    # for some reason it was impossible to access the FTP
    # this class fixes it, which is copied from this SO
    # https://stackoverflow.com/questions/14659154/ftps-with-python-ftplib-session-reuse-required
    class MyFTP_TLS(FTP_TLS):
        """Explicit FTPS, with shared TLS session."""

        def ntransfercmd(self, cmd, rest=None):
            conn, size = FTP.ntransfercmd(self=self, cmd=cmd, rest=rest)
            if self._prot_p:
                conn = self.context.wrap_socket(
                    sock=conn,
                    server_hostname=self.host,
                    session=self.sock.session,
                )  # this is the fix
            return conn, size

    ftps = MyFTP_TLS(host=ftp_address, user=ftp_username, passwd=ftp_password)
    ftps.prot_p()

    return ftps


def retrieve_file_ftp(
    ftp_filepath: str,
    output_filepath: str,
):
    """
    Download and save a file from ICPAC's ftp server.

    Parameters
    ----------
    ftp_filepath : str
        path on the server where the file is located
    output_filepath : str
        path to save the file to

    Examples
    --------
    >>> retrieve_file_ftp(ftp_filepath=
    ... '/SharedData/gcm/seasonal/202101/' \
    ... 'PredictedRainfallProbbability-FMA2021_Jan2021.nc',
    ... output_filepath='example.nc')
    """
    load_dotenv()
    ftp_address = os.getenv("ICPAC_FTP_ADDRESS")
    ftp_username = os.getenv("ICPAC_FTP_USERNAME")
    ftp_password = os.getenv("ICPAC_FTP_PASSWORD")
    # question: is this the best method to set the vars and raise the error?
    if None in (ftp_address, ftp_username, ftp_password):
        raise RuntimeError("One of the ftp variables is not set")
    # TODO: ugly, is there a better method? if not doing, mypy complains
    assert ftp_address is not None, ftp_address
    assert ftp_username is not None, ftp_username
    assert ftp_password is not None, ftp_password
    ftps = connect_icpac_ftp(
        ftp_address=ftp_address,
        ftp_username=ftp_username,
        ftp_password=ftp_password,
    )
    with open(output_filepath, "wb") as f:
        ftps.retrbinary("RETR " + ftp_filepath, f.write)
