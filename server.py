import socket

BUFF_SIZE = 5098

def server():
    # host = 'localhost' #only for public ip
    # host = str(input('Host IP: ')) #for input

    # for local network ip
    hostname = socket.gethostname()
    host = socket.gethostbyname(hostname)

    port = int(input('Port: ')) #host port here

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((host, port))
    serversocket.listen(5)
    print("Server started, waiting for connections...")

    connection, address = serversocket.accept()
    print(f"Connection established with {address}")

    while True:        
        data = connection.recv(BUFF_SIZE).decode('utf-8')  # Receive response
        if not data:
            print("Client disconnected.")
            break
        
        print(f"Client response: {data}") #print response

        message = "Server Echoed: " + data.upper() + "\nServer says: " #echoes back what the client says but uppercased
        message += input('Enter message: ')
        connection.send(message.encode('utf-8'))  # Send message to the client

    connection.close()
    serversocket.close()

server()