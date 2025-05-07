import socket
import sys

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

def quit_program():
    print('Closing program...')
    sys.exit()

def client():
    serverIP = input('Server IP: ')
    serverPort = int(input('Server port: '))

    print('Attempting to connect...')
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((serverIP, serverPort))

    print('Connected.')

    while True:
        selected = 0
        message = ''

        while True:
            message = input(str('Enter message: '))
            message = message.lower()
            if message == 'quit':
                quit_program()
            elif message not in valid_queries:
                # print(f'ECHOED: {message}')
                print('Sorry, this query cannot be processed. Please try one of the following:')
                print(f'- What is the average moisture inside my kitchen fridge in the past three hours?')
                print(f'- What is the average water consumption per cycle in my smart dishwasher?')
                print(f'- Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?')
                print()
            else:
                if message in valid_queries[0:2]:
                    selected = 1
                elif message in valid_queries[2:4]:
                    selected = 2
                elif message in valid_queries[4:8]:
                    selected = 3
                else: 
                    print('There was a problem reading the message, please try again.')
                    continue
                # print(f'Success! Message chosen: {selected}')
                break
        
        clientsocket.send(str(selected).encode('utf-8'))
        serverResponse = clientsocket.recv(BUFF_SIZE)
        print(f"Server response: {serverResponse.decode('utf-8')}")

client()
