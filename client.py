import socket

BUFF_SIZE = 5098

valid_queries = [
    'what is the average moisture inside my kitchen fridge in the past three hours?',
    'what is the average moisture inside my kitchen fridge in the past three hours',
    'what is the average water consumption per cycle in my smart dishwasher?',
    'what is the average water consumption per cycle in my smart dishwasher',
    'which device consumed more electricity among my three iot devices (two refrigerators and a dishwasher)?',
    'which device consumed more electricity among my three iot devices (two refrigerators and a dishwasher)',
    'which device consumed more electricity among my three iot devices?',
    'which device consumed more electricity among my three iot devices',
]

def client():
    serverIP   = input('Server IP: ')
    serverPort = int(input('Server port: '))

    print('Attempting to connect...')
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((serverIP, serverPort))
    print('Connected.')

    while True:
        # ask until they enter a valid query
        while True:
            message = input('Enter message: ').strip().lower()
            if message not in valid_queries:
                print('Sorry, this query cannot be processed. Please try one of the following:')
                print('- What is the average moisture inside my kitchen fridge in the past three hours?')
                print('- What is the average water consumption per cycle in my smart dishwasher?')
                print('- Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?\n')
                continue
            break

        # map text to 1/2/3
        if message in valid_queries[0:2]:
            code = '1'
        elif message in valid_queries[2:4]:
            code = '2'
        else:
            code = '3'

        clientsocket.send(code.encode('utf-8'))
        serverResponse = clientsocket.recv(BUFF_SIZE).decode('utf-8')
        print(f"Server response: {serverResponse}\n")

if __name__ == '__main__':
    client()
