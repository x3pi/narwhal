#!/usr/bin/env python3
"""
Validator Proxy - Trả về validator list cho Narwhal từ committee file
"""
import socket
import struct
import sys
import os
import json
import time
from pathlib import Path

def create_validator_list_response(committee_data):
    """
    Tạo ValidatorList response từ committee.json
    Format: ValidatorList { validators: [Validator] }
    Hiện tại chỉ trả về empty list để hệ thống có thể khởi động
    """
    # Trả về empty validators list với proper encoding
    response = bytearray()
    # Empty list không cần encode gì cả
    # Chỉ cần response rỗng là đủ
    
    return bytes(response)

def handle_connection(conn, addr):
    print(f"New connection from {addr}")
    try:
        while True:
            # Read message length (4 bytes, big-endian)
            len_buf = conn.recv(4)
            if len(len_buf) < 4:
                break
            msg_len = struct.unpack('>I', len_buf)[0]
            print(f"Received request length: {msg_len}")
            
            # Read message data
            data = conn.recv(msg_len)
            if len(data) < msg_len:
                break
            
            print(f"Received {len(data)} bytes of request data")
            
            # Create response - trả về empty ValidatorList
            response = create_validator_list_response(None)
            print(f"Created response with {len(response)} bytes")
            
            # Send response length
            conn.send(struct.pack('>I', len(response)))
            # Send response (empty là OK cho epoch 1)
            if response:
                conn.send(response)
            
            print(f"Sent response successfully")
            
    except BrokenPipeError:
        print(f"Client disconnected: {addr}")
    except Exception as e:
        print(f"Error in handle_connection: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            conn.close()
        except:
            pass
        print(f"Connection closed for {addr}")

def main():
    socket_path = "/tmp/get_validator.sock_1"
    
    # Remove existing socket
    if os.path.exists(socket_path):
        os.unlink(socket_path)
        print(f"Removed existing socket: {socket_path}")
    
    # Create socket
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    
    try:
        server.bind(socket_path)
        server.listen(5)
        os.chmod(socket_path, 0o666)
        print(f"Validator proxy listening on {socket_path}")
        print("Waiting for connections...")
        
        while True:
            conn, addr = server.accept()
            handle_connection(conn, addr)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        server.close()
        if os.path.exists(socket_path):
            os.unlink(socket_path)

if __name__ == "__main__":
    main()