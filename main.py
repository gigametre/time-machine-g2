import serial
from datetime import datetime
from pathlib import Path
from TimeMachineClient import TimeMachineClient
from logging_utils import get_session_logger

# Configure logging using SessionLogger
session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
session_dir = Path("logs") / f"session_{session_id}"
session_dir.mkdir(parents=True, exist_ok=True)
logger = get_session_logger(session_dir)


def fetch_event_data(event_num: int = 1):
    logger.info(f"Starting TimeMachineClient event {event_num} read", component="main")

    tm = TimeMachineClient(
        port='COM4',
        baudrate=9600,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=1.0,
        inter_byte_delay=0.01,
        logger=logger,
    )

    try:
        text = tm.download_memory(event_num=event_num, heat_num=0, start_time=None, read_seconds=10.0)
        print(f"=== EVENT {event_num} DATA START ===")
        print(text)
        print(f"=== EVENT {event_num} DATA END ===")

        logging.info(f"Event {event_num} data received")
        logger.log_data("event_data", text, component="main")

        return text

    except Exception as e:
        print(f"Error reading event {event_num} data: {e}")
        logger.error(f"Error reading event {event_num} data", component="main", exc_info=True)
        return ""

    finally:
        tm.close()
        logger.info("TimeMachineClient closed", component="main")


def main():
    tm = TimeMachineClient(
        port='COM4',
        baudrate=9600,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=1.0,
        logger=logger,
        inter_byte_delay=0.01,
    )

    t = tm.fetch_event_data(event_num=2)

    print("=== FULL EVENT 2 DATA ===")
    print(t)

if __name__ == '__main__':
    main()

