import logging
import socket
import datetime as dt
from datapacket import ForzaDataPacket
import argparse
import time
from kinesisStream import KinesisStream
import boto3

cli_parser = argparse.ArgumentParser(
    description="script that grabs data from a Forza Horizon 5 stream and dumps it to a json file"
)

cli_parser.add_argument('-p', '--port', type=int, help='port number to listen on', default=65530)
cli_parser.add_argument('-v', '--verbose', action='store_true', help='display logs', default=False)
cli_parser.add_argument('-f', '--output-filename', default='out.log')
cli_parser.add_argument('-m', '--mode', choices=['race', 'always'], help='when to log: always or only during races.', default='always')
cli_parser.add_argument('-k', '--kinesisstreamname', help='name of kinesis stream', default='forza-stream')
args = cli_parser.parse_args()

k_stream_name = args.kinesisstreamname
client = boto3.client('kinesis')
k_stream = KinesisStream(client)
k_stream.name = k_stream_name

limiter=0
limit=10

def to_str(value):
    if isinstance(value, float):
        return('{:f}'.format(value))

    return('{}'.format(value))

def ratelimit(time,limit):
    global limiter
    if limit == 0:
        return True
    if time >= (limiter + limit):
        limiter = time
        return True
    return False

def dump_stream(port):

    # Get connection to db.
    
    params = ForzaDataPacket.get_props()

    log_wall_clock = False
    if 'wall_clock' in params:
        log_wall_clock = True


    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind(('', port))

    logging.info('PORT {} LISTEN'.format(port))

    n_packets = 0
    
    while True:
        message, address = server_socket.recvfrom(1024)
        del address
        fdp = ForzaDataPacket(message)
        fdp.wall_clock = dt.datetime.now()
        
        now = time.time()
        
        # Get all properties
        properties = fdp.get_props()
        # Get parameters
        data = fdp.to_list(params)
        assert len(data) == len(properties)
        # Zip into a dictionary
        fd_dict = dict(zip(properties, data))
        # Add timestamp
        fd_dict['timestamp'] = str(fdp.wall_clock)
        ratelimit_bool = ratelimit(now,limit)
        # only log if it's in a race.
        if args.mode == 'race' and ratelimit_bool:
            if fdp.is_race_on:
                if n_packets == 0:
                    logging.info('{}: in race, logging data'.format(dt.datetime.now()))
                
                logging.debug(fd_dict)
                #dbhandler.insert('data', fd_dict)
                k_stream.put_record(fd_dict,"forza-stream")

                n_packets += 1
                if n_packets % 60 == 0:
                    logging.info('{}: logged {} packets'.format(dt.datetime.now(), n_packets))
            else:
                if n_packets > 0:
                    logging.info('{}: out of race, stopped logging data'.format(dt.datetime.now()))
                n_packets = 0

        elif args.mode == 'always' and ratelimit_bool:
            k_stream.put_record(fd_dict,"forza-stream")
            logging.debug(fd_dict)
        
        
    



def main():
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    dump_stream(args.port)



if __name__ == "__main__":
    main()