import socket
import threading
import json

# Peer configuration
HOST = '127.0.0.1'  # Change to your machine's IP if needed
PORT = int(input("Enter the port to listen on: "))  # User enters the port
peers = []          # List of connected peers (IP, PORT)

# Function to handle incoming messages
def handle_client(conn, addr):
    print(f"[INFO] Connected by {addr}")
    try:
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break
            print(f"[MESSAGE] Received from {addr}: {data}")
            # Optionally send an acknowledgment back
            conn.sendall("ACK".encode('utf-8'))
    except ConnectionResetError:
        print(f"[INFO] Connection with {addr} lost")
    finally:
        conn.close()

# Function to start the server (listens for incoming connections)
def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

# Function to send messages to a peer
def send_message(target_host, target_port, message):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((target_host, target_port))
        client_socket.sendall(message.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        print(f"[RESPONSE] From {target_host}:{target_port} -> {response}")
    except ConnectionRefusedError:
        print(f"[ERROR] Could not connect to {target_host}:{target_port}")
    finally:
        client_socket.close()

# Function to add a peer to the list
def add_peer(ip, port):
    peers.append((ip, port))
    print(f"[PEER] Added peer {ip}:{port}")

# Main function to run server and send messages
if __name__ == "__main__":
    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()
    
    print("[INFO] Server started. Enter 'send', 'peers', or 'exit'")
    while True:
        command = input(">> ").strip()
        if command == "send":
            # Input target peer and message
            target_ip = input("Enter target IP: ")
            target_port = int(input("Enter target PORT: "))
            message = input("Enter message: ")
            send_message(target_ip, target_port, message)
        elif command == "peers":
            print("[PEERS] Connected peers:")
            for p in peers:
                print(f"- {p[0]}:{p[1]}")
        elif command == "exit":
            print("[INFO] Exiting...")
            break
