""" various deprecated helper functions """
import os


def get_utc_environ():
    """
    Creates a copy of this process' environment variables with the timezone
    variable set to UTC and returns it.

    Returns:
        A copy of os.environ with "TZ" set to "UTC".
    """
    utc_environ = os.environ.copy()
    utc_environ["TZ"] = "UTC"
    return utc_environ


def log_pipe(pipe, logging_function, template="%s"):
    """
    Logs each non-empty line from a pipe (or other file-like object)
    using the given logging function. This will block until the end of
    the pipe is reached.

    Args:
        pipe: The pipe to read from.
        logging_function: The logging function to use.
        template: (Optional) A template string to place each line from pipe
                  inside.
    """
    if (not pipe) or (not logging_function):
        return

    for line in pipe:
        stripped_line = line.rstrip()
        if stripped_line:
            logging_function(template % stripped_line)


def exists_ok_makedirs(path):
    """
    A wrapper for os.makedirs that does not throw an exception
    if the given path points to an existing directory.

    Args:
        path: The path to the directory to create.
    Throws:
        EnvironmentError: Thrown if the directory could not be created.
    """

    try:
        os.makedirs(path)
    except EnvironmentError:
        if not os.path.isdir(path):
            raise
