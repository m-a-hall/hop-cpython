##
##   This program is free software: you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as published by
##   the Free Software Foundation, either version 3 of the License, or
##   (at your option) any later version.
##
##   This program is distributed in the hope that it will be useful,
##   but WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public License
##   along with this program.  If not, see <http://www.gnu.org/licenses/>.
##

__author__ = 'mhall'

import sys
import socket
import struct
import os
import json
import base64
import math
import traceback
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import csv

# Try to import pyarrow for Arrow support
_global_arrow_available = False
try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
    _global_arrow_available = True
except ImportError:
    pass

_global_python3 = sys.version_info >= (3, 0)

if _global_python3:
    from io import StringIO
    from io import BytesIO
else:
    try:
        from cStringIO import StringIO
    except:
        from StringIO import StringIO

try:
    import cPickle as pickle
except:
    import pickle

_global_connection = None
_global_env = {}
_global_use_arrow = False  # Flag to control Arrow usage

_global_startup_debug = False

# _global_std_out = StringIO()
# _global_std_err = StringIO()
sys.stdout = StringIO()
sys.stderr = StringIO()

if len(sys.argv) > 2:
    if sys.argv[2] == 'debug':
        _global_startup_debug = True


def runServer():
    if _global_startup_debug == True:
        print('Python server starting...\n')
        print('Arrow support available: %s\n' % _global_arrow_available)
    global _global_connection
    _global_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _global_connection.connect(('localhost', int(sys.argv[1])))
    pid_response = {}
    pid_response['response'] = 'pid_response'
    pid_response['pid'] = os.getpid()
    pid_response['arrow_available'] = _global_arrow_available
    send_response(pid_response, True)
    try:
        while 1:
            message = receive_message(True)
            if 'command' in message:
                command = message['command']
                # Check if we should use Arrow for this session
                if 'use_arrow' in message:
                    global _global_use_arrow
                    _global_use_arrow = message['use_arrow'] and _global_arrow_available
                
                if command == 'accept_rows':
                    if _global_use_arrow and _global_arrow_available:
                        receive_rows_arrow(message)
                    else:
                        receive_rows(message)
                elif command == 'get_frame':
                    if _global_use_arrow and _global_arrow_available:
                        send_rows_arrow(message)
                    else:
                        send_rows(message)
                elif command == 'execute_script':
                    execute_script(message)
                elif command == 'get_variable_list':
                    send_variable_list(message)
                elif command == 'get_variable_type':
                    send_variable_type(message)
                elif command == 'get_variable_value':
                    send_variable_value(message)
                elif command == 'get_image':
                    send_image_as_png(message)
                elif command == 'variable_is_set':
                    send_variable_is_set(message)
                elif command == 'set_variable_value':
                    receive_variable_value(message)
                elif command == 'get_debug_buffer':
                    send_debug_buffer()
                elif command == 'shutdown':
                    if _global_startup_debug == True:
                        print ('Received shutdown command...\n')
                    exit()
            else:
                if _global_startup_debug == True:
                    print('message did not contain a command field!')
    finally:
        _global_connection.close()


def message_debug(message):
    if 'debug' in message:
        return message['debug']
    else:
        return False


def send_debug_buffer():
    tOut = sys.stdout
    tErr = sys.stderr
    ok_response = {}
    ok_response['response'] = 'ok'
    ok_response['std_out'] = tOut.getvalue()
    ok_response['std_err'] = tErr.getvalue()
    # clear the buffers
    tOut.close()
    tErr.close()
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    send_response(ok_response, True)


def receive_rows_arrow(message):
    """Receive rows using Apache Arrow format"""
    if 'row_meta' in message:
        row_meta = message['row_meta']
        frame_name = row_meta['frame_name']
        num_rows = message['num_rows']
        
        if num_rows > 0:
            # Receive Arrow IPC stream
            size_bytes = b''
            while len(size_bytes) < 4:
                size_bytes += _global_connection.recv(4 - len(size_bytes))
            size = struct.unpack('>L', size_bytes)[0]
            
            # Read the Arrow data
            arrow_data = b''
            while len(arrow_data) < size:
                chunk = _global_connection.recv(min(size - len(arrow_data), 65536))
                if not chunk:
                    break
                arrow_data += chunk
            
            # Convert Arrow to pandas DataFrame
            reader = pa.ipc.open_stream(BytesIO(arrow_data))
            table = reader.read_all()
            frame = table.to_pandas()
            
            _global_env[frame_name] = frame
            
            if message_debug(message) == True:
                print(frame.info(), '\n')
                print(frame, '\n')
        
        ack_command_ok()
    else:
        error = 'put rows json message does not contain a header entry!'
        ack_command_err(error)


def send_rows_arrow(message):
    """Send rows using Apache Arrow format"""
    frame_name = message['frame_name']
    frame = get_variable(frame_name)
    include_index = False
    if 'include_index' in message:
        include_index = message['include_index']
    
    if type(frame) is not pd.DataFrame:
        message = 'Variable ' + frame_name
        if frame is None:
            message += ' is not defined'
        else:
            message += ' is not a DataFrame object'
        ack_command_err(message)
        return
    else:
        ack_command_ok()
    
    # Send metadata response
    response = {}
    response['frame_name'] = frame_name
    response['response'] = 'row_meta'
    response['num_rows'] = len(frame.index)
    response['fields'] = frame_to_fields_list(frame, include_index)
    response['format'] = 'arrow'
    if message_debug(message) == True:
        print(response)
    send_response(response, True)
    
    # Convert DataFrame to Arrow and send as IPC stream
    if include_index:
        frame_with_index = frame.reset_index()
        table = pa.Table.from_pandas(frame_with_index, preserve_index=False)
    else:
        table = pa.Table.from_pandas(frame, preserve_index=False)
    
    # Write to buffer
    sink = BytesIO()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    
    # Send the Arrow data
    arrow_bytes = sink.getvalue()
    _global_connection.sendall(struct.pack('>L', len(arrow_bytes)))
    _global_connection.sendall(arrow_bytes)


def receive_rows(message):
    if 'row_meta' in message:
        row_meta = message['row_meta']
        frame_name = row_meta['frame_name']
        num_rows = message['num_rows']
        b64e = message['base64'];
        frame = None
        if num_rows > 0:
            # receive the CSV
            csv_data = receive_message(False)
            if b64e is True:
                # For some reason decoding byte stream that has been encoded to utf-8 by Java causes
                # the python decoder to hang when there are non-ascii characters present.
                # Encoding to base64 on the Java side, then decoding base64 and then
                # to utf-8 on the python side seems to rectify this. Due to the overhead of base64, we only
                # do this when non-ascii characters are detected.
                csv_data = base64_decode(csv_data)
                if _global_python3 is True:
                    csv_data = csv_data.decode('utf-8', 'ignore')

            frame = pd.read_csv(StringIO(csv_data), na_values='?',
                                quotechar='\'', escapechar='\\',
                                index_col=None)
            _global_env[frame_name] = frame
            # convert any date longs to date objects and
            # any boolean strings to True/False
            for field in row_meta['fields']:
                field_name = field['name']
                field_type = field['type']
                if field_type == 'boolean':
                    frame[field_name] = (frame[field_name] == 1)
                elif field_type == 'date':
                    frame[field_name] = pd.to_datetime(frame[field_name],unit='ms')
            if message_debug(message) == True:
                print(frame.info(), '\n')
                print (frame, '\n')
        ack_command_ok()
    else:
        error = 'put rows json message does not contain a header entry!'
        ack_command_err(error)

def send_rows(message):
    frame_name = message['frame_name']
    frame = get_variable(frame_name)
    include_index = False
    if 'include_index' in message:
        include_index = message['include_index']
    if type(frame) is not pd.DataFrame:
        message = 'Variable ' + frame_name
        if frame is None:
            message += ' is not defined'
        else:
            message += ' is not a DataFrame object'
        ack_command_err(message)
    else:
        ack_command_ok()
    response = {}
    response['frame_name'] = frame_name
    response['response'] = 'row_meta'
    response['num_rows'] = len(frame.index)
    response['fields'] = frame_to_fields_list(frame, include_index)
    if message_debug(message) == True:
        print(response)
    send_response(response, True)
    s = StringIO()
    frame.to_csv(path_or_buf=s, na_rep='?', doublequote=False, index=include_index,
                 quotechar='\'', line_terminator='#||#', quoting=csv.QUOTE_NONNUMERIC,
                 escapechar='\\', header=False, date_format='%Y-%m-%d %H:%M:%S.%f')
    send_response(s.getvalue(), False)

def frame_to_fields_list(frame, include_index):
    fields = []
    if include_index:
        # add the index as a field
        # Python 3 returns the name of an index. Python 2 returns True if the
        # index has a name, or None otherwise
        if _global_python3 and frame.index.name is not None:
            name = frame.index.name
        else:
            name = "index"
        fields.append({'name': name, 'type': 'number'})

    for n in frame.columns:
        t = 'string'
        d = frame[n].dtype
        if d == 'float16' or d == 'float32' or d == 'float64':
            t = 'number'
        elif d == 'int8' or d == 'int16' or d == 'int32' or d == 'int64':
            t = 'number'
        elif d == 'bool':
            t = 'boolean'
        elif d.name.startswith('datetime'):
            t = 'date'
        fields.append({'name': n, 'type': t})
    return fields


def send_response(response, isJson):
    if isJson is True:
        response = json.dumps(response)

    if _global_python3 is True:
        response_bytes = response.encode('utf-8')
        _global_connection.sendall(struct.pack('>L', len(response_bytes)))
        _global_connection.sendall(response_bytes)
    else:
        # For Python 2, ensure we're sending the byte length, not string length
        if isinstance(response, unicode):
            response_bytes = response.encode('utf-8')
        else:
            # Ensure it's encoded as UTF-8 bytes
            response_bytes = response.encode('utf-8') if isinstance(response, str) else response
        _global_connection.sendall(struct.pack('>L', len(response_bytes)))
        _global_connection.sendall(response_bytes)


def receive_message(isJson):
    size = 0
    length = None
    if _global_python3 is True:
        length = bytearray()
    else:
        length = ''
    while len(length) < 4:
        if _global_python3 is True:
            length += _global_connection.recv(4);
        else:
            length += _global_connection.recv(4);

    size = struct.unpack('>L', length)[0]

    data = ''
    while len(data) < size:
        if _global_python3 is True:
            data += _global_connection.recv(size).decode('ascii',"backslashreplace");
        else:
            data += _global_connection.recv(size);
    if isJson is True:
        return json.loads(data)
    return data


def ack_command_err(message):
    err_response = {}
    err_response['response'] = 'error'
    err_response['error_message'] = message
    send_response(err_response, True)


def ack_command_ok():
    ok_response = {}
    ok_response['response'] = 'ok'
    send_response(ok_response, True)


def get_variable(var_name):
    if var_name in _global_env:
        return _global_env[var_name]
    else:
        return None


def execute_script(message):
    if 'script' in message:
        script = message['script']
        tOut = sys.stdout
        tErr = sys.stderr
        output = StringIO()
        error = StringIO()
        if message_debug(message):
            print('Executing script...\n\n' + script)
        sys.stdout = output
        sys.stderr = error
        try:
            exec (script, _global_env)
        except Exception:
            print('Got an exception executing script')
            traceback.print_exc(file=error)
        sys.stdout = tOut
        sys.stderr = tErr
        # sys.stdout = sys.__stdout__
        # sys.stderr = sys.__stderr__
        ok_response = {}
        ok_response['response'] = 'ok'
        ok_response['script_out'] = output.getvalue()
        ok_response['script_error'] = error.getvalue()
        send_response(ok_response, True)
    else:
        error = 'execute script json message does not contain a script entry!'
        ack_command_err(error)


def send_variable_is_set(message):
    if 'variable_name' in message:
        var_name = message['variable_name']
        var_value = get_variable(var_name)
        ok_response = {}
        ok_response['response'] = 'ok'
        ok_response['variable_name'] = var_name
        if var_value is not None:
            ok_response['variable_exists'] = True
        else:
            ok_response['variable_exists'] = False
        send_response(ok_response, True)
    else:
        error = 'object exists json message does not contain a variable_name entry!'
        ack_command_err(error)


def send_variable_type(message):
    if 'variable_name' in message:
        var_name = message['variable_name']
        var_value = get_variable(var_name)
        if var_value is None:
            ack_command_err('variable ' + var_name + ' is not set!')
        else:
            ok_response = {}
            ok_response['response'] = 'ok'
            ok_response['variable_name'] = var_name
            ok_response['type'] = 'unknown'
            if type(var_value) is pd.DataFrame:
                ok_response['type'] = 'dataframe'
            elif type(var_value) is plt.Figure:
                ok_response['type'] = 'image'
            send_response(ok_response, True)
    else:
        ack_command_err(
            'send variable type json message does not contain a variable_name entry!')


def send_variable_value(message):
    if 'variable_encoding' in message:
        encoding = message['variable_encoding']
        if encoding == 'pickled' or encoding == 'json' or encoding == 'string':
            send_encoded_variable_value(message)
        else:
            ack_command_err(
                'Unknown encoding type for send variable value message')
    else:
        ack_command_err('send variable value message does not contain an '
                        'encoding field')


def send_variable_list(message):
    variables = []
    for key, value in dict(_global_env).items():
        variable_type = type(value).__name__
        if not (
                            variable_type == 'classob' or variable_type == 'module' or variable_type == 'function'):
            variables.append({'name': key, 'type': variable_type})
    ok_response = {}
    ok_response['response'] = 'ok'
    ok_response['variable_list'] = variables
    send_response(ok_response, True)


def base64_encode(value):
    # encode to base 64 bytes
    b64 = base64.b64encode(value)
    # get it as a string
    b64s = b64
    if _global_python3 is True:
        b64s = b64.decode('utf8')
    return b64s


def base64_decode(value):
    b64b = value
    if _global_python3 is True:
        # from string to bytes
        b64b = value.encode()
    # back to non-base64 bytes
    bytes = base64.b64decode(b64b)
    return bytes


def image_as_encoded_string(value):
    # return image as png data encoded in a string.
    # assumes image is a matplotlib.figure.Figure
    encoded = None
    if _global_python3:
        sio = BytesIO()
        value.savefig(sio, format='png')
        encoded = base64_encode(sio.getvalue())
    else:
        sio = StringIO()
        value.savefig(sio, format='png')
        encoded = base64_encode(sio.getvalue())
    return encoded


def send_image_as_png(message):
    if 'variable_name' in message:
        var_name = message['variable_name']
        image = get_variable(var_name)
        if image is not None:
            if type(image) is plt.Figure:
                ok_response = {}
                ok_response['response'] = 'ok'
                ok_response['variable_name'] = var_name
                # encoding = 'plain'
                # if _global_python3 is True:
                encoding = 'base64'
                ok_response['encoding'] = encoding
                ok_response['image_data'] = image_as_encoded_string(image)
                if message_debug(message) == True:
                    print(
                        'Sending ' + var_name + ' base64 encoded as png bytes')
                send_response(ok_response, True)
            else:
                ack_command_err(
                    var_name + ' is not a matplot.figure.Figure object')
        else:
            ack_command_err(var_name + ' does not exist!')
    else:
        ack_command_err(
            'get image json message does not contain a variable_name entry!')


def send_encoded_variable_value(message):
    if 'variable_name' in message:
        var_name = message['variable_name']
        object = get_variable(var_name)
        if object is not None:
            encoding = message['variable_encoding']
            encoded_object = None
            if encoding == 'pickled':
                encoded_object = pickle.dumps(object)
                if _global_python3 is True:
                    encoded_object = base64_encode(encoded_object)
            elif encoding == 'json':
                encoded_object = object  # the whole response gets serialized to json
            elif encoding == 'string':
                encoded_object = str(object)
            ok_response = {}
            ok_response['response'] = 'ok'
            ok_response['variable_name'] = var_name
            ok_response['variable_encoding'] = encoding
            ok_response['variable_value'] = encoded_object
            if message_debug(message) == True:
                print(
                    'Sending ' + encoding + ' value for var ' + var_name + "\n")

            send_response(ok_response, True)
        else:
            ack_command_err(var_name + ' does not exist!')
    else:
        ack_command_err(
            'get variable value json message does not contain a variable_name entry!')


def receive_variable_value(message):
    if 'variable_encoding' in message:
        if message['variable_encoding'] == 'pickled':
            receive_pickled_variable_value(message)
    else:
        ack_command_err('receive variable value message does not contain an '
                        'encoding field')


def receive_pickled_variable_value(message):
    if 'variable_name' in message and 'variable_value' in message:
        var_name = message['variable_name']

        pickled_var_value = message['variable_value']
        # print("Just before de-pickling")
        # print(pickled_var_value)
        if _global_python3:
            pickled_var_value = base64_decode(pickled_var_value)
        var_value = pickle.loads(pickled_var_value)
        _global_env[var_name] = var_value
        ack_command_ok()
    else:
        ack_command_err('receive pickled variable value does not contain a variable_name '
                        'and/or variable_value entry')


def is_json(json_data):
    try:
        json_object = json.loads(json_data)
    except:
        return False


runServer()