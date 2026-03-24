import serial
import time


def write_slow(ser, data, delay=0.01):
    for b in data:
        ser.write(bytes([b]))
        ser.flush()
        time.sleep(delay)


ser = serial.Serial(
    port="COM4",
    baudrate=9600,
    bytesize=serial.EIGHTBITS,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    timeout=0.5,
)

time.sleep(1)

# XON
#write_slow(ser, bytes([0x11]))

# RETRANSMIT all events / all heats:
# 0x05 + "00000" + CRLF
cmd = bytes([0x05]) + b"00000\r\n"
write_slow(ser, cmd)

print("Sent retransmit command")

end_time = time.time() + 8
buffer = bytearray()

while time.time() < end_time:
    waiting = ser.in_waiting
    if waiting:
        chunk = ser.read(waiting)
        buffer.extend(chunk)
        print("RX chunk:", chunk)
    else:
        time.sleep(0.05)

ser.close()

print("\nFull response:")
print(buffer.decode("ascii", errors="replace"))