import serial
import logging

def send_0x80_zeroes(ser):
    """
    Send command: 0x80 + six ASCII '0' + CR/LF on COM4.
    """
    message = b'\x80' + b'000000' + b'\r\n'
    ser.write(message)
    logging.info("Sent 0x80 + 6x '0' + CR/LF")
    # read immediate response from device
    response = ser.readline().decode('utf-8', errors='ignore').strip()
    if response:
        print(f"Response: {response}")
        logging.info(f"Response: {response}")


if __name__ == "__main__":
    logging.basicConfig(
        filename='serial_communication.log',
        level=logging.INFO, 
        format='%(asctime)s - %(message)s'
    )


    try:        # Open COM port 4
        ser = serial.Serial('COM4', baudrate=9600, timeout=1)
        print(f"Connected to {ser.port}")
        logging.info(f"Connected to {ser.port}")
        send_0x80_zeroes(ser)
        

    except serial.SerialException as e:
        print(f"Error: {e}")
        logging.error(f"Serial error: {e}")