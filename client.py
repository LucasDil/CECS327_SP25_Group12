import socket

BUFF_SIZE = 5098

def client():
    serverIP = input('Server IP: ')
    serverPort = int(input('Server port: '))

    print('Attempting to connect...')
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((serverIP, serverPort))

    print('Connected.')

    while True:
        message = input('Enter message: ')
        clientsocket.send(message.encode('utf-8'))
        serverResponse = clientsocket.recv(BUFF_SIZE)
        print(f"Server response: {serverResponse.decode('utf-8')}")

client()
