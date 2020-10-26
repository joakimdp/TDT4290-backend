
import logging
import os


def setup_logging() -> object:
    log_filename = 'logs/output.log'
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    logging.basicConfig(filename=log_filename,
                        filemode='a',
                        format='%(asctime)s,%(msecs)d - %(levelname)s - %(message)s',
                        datefmt='%d.%m.%Y %H:%M:%S',
                        level=logging.DEBUG)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter(
        '%(asctime)s,%(msecs)d - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    return logging
