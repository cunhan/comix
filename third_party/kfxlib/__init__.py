from . import message_logging
from . import utilities
from . import yj_book
from . import yj_metadata


__license__ = "GPL v3"
__copyright__ = "2016-2025, John Howell <jhowell@acm.org>"


set_logger = message_logging.set_logger
JobLog = message_logging.JobLog
YJ_Book = yj_book.YJ_Book
YJ_Metadata = yj_metadata.YJ_Metadata
KFXDRMError = utilities.KFXDRMError


clean_message = utilities.clean_message
file_read_binary = utilities.file_read_binary
file_write_binary = utilities.file_write_binary
file_read_utf8 = utilities.file_read_utf8
file_write_utf8 = utilities.file_write_utf8
json_deserialize = utilities.json_deserialize
json_serialize = utilities.json_serialize
unicode_argv = utilities.unicode_argv
windows_long_path_fix = utilities.windows_long_path_fix

IS_LINUX = utilities.IS_LINUX
IS_MACOS = utilities.IS_MACOS
IS_WINDOWS = utilities.IS_WINDOWS

user_home_dir = utilities.user_home_dir
windows_user_dir = utilities.windows_user_dir
