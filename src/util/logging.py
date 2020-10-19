
import logging


def setup_logging() -> object:
    logging.basicConfig(filename='logs/output.log',
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
