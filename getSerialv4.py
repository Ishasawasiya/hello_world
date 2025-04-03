##################################################

##################################################
"""
update relay ids in smv summary reports using key columns 
usage:
python getSerialv3.py /path/to/log/file.log path/to/folder/with/summary/reports/ serial_column_name path/to/udap/data/file*

NOTE: this script appends relay-ids to unique_relays.csv
"""
from xlWriter import createWorkBook

import sys
if len(sys.argv) < 5:
    raise TypeError('Too few arguments passed: Exactly 4 arguments are required.')

logFilePath = sys.argv[1]
reportsDir = sys.argv[2]
serialColumnName = sys.argv[3]
udapDataFilePath = ' '.join(sys.argv[4:])

SED_TXT_SIZE = 10*3 # after finding keycolumn match line#, how many lines to check for finding udap serial number
BACKUP_REPORT_FILE = True
PLATFORM = 'udap'
# awk script to get N_LINES below the first line that matches <search_key> and then immediately exit
AWK_SCRIPT = r"BEGIN {x=0}{if (x > 0){if (x < <N_LINES>){print $0;x += 1}else {exit}}else {if ($0 ~ /<search_key>/){x=1} else {}}}"

# output formatting
REPORT_HEADER = ['Key Column', 'Action', 'Comments']
COLUMN_WIDTHS = [105, 40, 40]

import subprocess
# function to run shell command
def shellExec(args, shell=False):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
    stdout, stderr = p.communicate()
    p.wait()
    return stdout, stderr

def getSerialFromText(text):
    serial_num = '' # this is what will be returned if the search was unsuccessful
    for line in text.splitlines():
        if ((PLATFORM in line) and (serialColumnName in line)):
            serial_num = line.split(' : ')[2]
            break
    else:
        print(text)
        raise RuntimeError('Search failed. Either wrong file paths are given or SED_TXT_SIZE too small.')
    return serial_num

from string import printable
def decode(text):
    result = ''
    for c in text:
        if c in printable[:-2]:
            if c == '&':
                result += '&amp;'
            else:
                result += c 
        else:
            c_hex = hex(ord(c))
            result += '[{}]'.format(c_hex)
    return result

def read(filename, delim=None):
    data = []
    with open(filename, 'rb') as f:
        for _line in f.read().splitlines() :
            line = decode(_line)
            if delim:
                data.append(line.split(delim))
            else:
                data.append([line])
    return data 

import time
start_time = time.time()

# import sys
import os
text, err = shellExec('ls ' + reportsDir, shell=True)
allFiles = [f for f in text.splitlines()]
reportFileNames = [f for f in allFiles if f.endswith('.1')]

relays = set()

summary_report_dict = {}

for report in reportFileNames:
    path = reportsDir + '/' + report
    sys.stdout.write('report path :' + path + '\n')
    report_content = read(path)
    
    keys = {row[0] for row in report_content if row[0][:3]=='Key'}
    if len(keys) == 0:
        continue

    # Create key to serial mapping
    key_serial_dict = {}
    N = len(keys)
    i = 0
    for key in keys:
        sys.stdout.write('Getting serial values: ')
        # we use awk command as it can search and manipulation of search results
        args = [
            'awk',
            AWK_SCRIPT.replace(r'<search_key>', key.replace('XXXXXX', '[0-9]{6}')).replace(r'<N_LINES>', str(SED_TXT_SIZE)),
            logFilePath
        ]

        text, err = shellExec(args) 

        if len(err): # why would you get
            raise RuntimeError(err)
                
        serial = getSerialFromText(text)
        key_serial_dict.update({key: serial})

        i += 1
        prg = (i*100//N)
        sys.stdout.write(str(prg) + '%\n')

    sys.stdout.write('... Done.\nGetting relay-ids: ')

    prefix = 'gzip -dc {} | cut -c1-13,424-496 | grep -e \''.format(udapDataFilePath)
    command = prefix + '\' -e \''.join(key_serial_dict.values()) + '\''

    text, err = shellExec(command, shell=True)
    sys.stdout.write('Done.\n')

    # Create serial to relay mapping
    serial_relay_dict = {}
    for line in text.splitlines():
        if len(line):
            serial, relay = line.split()
            relays.add(relay)
            serial_relay_dict.update({serial: relay})

    # JOIN TABLES: key_serial and serial_relay 
    for row in report_content:
        line = row[0]
        if line[:3] == 'Key':
            serial = key_serial_dict[line]
            relay = serial_relay_dict[serial]
            line += '-' + relay
            row[0] = line # switch
        
    summary_report_dict.update(
        {
            report : {
                'table': [REPORT_HEADER] + report_content,
                'header': True,
                'columnWidths': COLUMN_WIDTHS
            }
        }
    )

export_id = len([f for f in allFiles if f.endswith('.xlsx')])
export_path = reportsDir + '/summary_report_{}.xlsx'.format(export_id)
print('\nWriting to: ' + export_path)
createWorkBook(summary_report_dict, export_path)

t = int(time.time() - start_time)
t_str = str(t//60) + 'm ' + str(t%60) + 's'

print('total time: ' +  t_str)