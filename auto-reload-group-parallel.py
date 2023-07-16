import socket
import time
import sys
import logging
import logging.handlers
import xmlrpclib
import datetime
import ast
import os
import cx_Oracle
from multiprocessing import Process
 
 
##LOGGING##
LOG_FILENAME="auto-reload.log"
log = logging.getLogger('OAM')
log.setLevel(logging.DEBUG)
# Add the log message handler to the logger
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when = "d", interval=1, backupCount=2)
# create a logging format
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
handler.setFormatter(formatter)
log.addHandler(handler)
######
 
log.debug("##############################Parallel Reload##############################")
log.debug("Loading parameters from input")
arg_names = ['exename', 'list_group_function', 'list_processExclude', 'list_IPExclude', 'params']
args = dict(zip(arg_names, sys.argv))
 
 
log.debug('Loaded params from input: ' + str(args))
if 'params' in args:
    args['params'] = args['params'].split('&')
    params = []
    for val in args['params']:
        if val.split("|")[1] == "Integer":
            params.append(int(val.split("|")[0]))
        else:
            params.append(val[0])
else:
    params = []
 
if 'list_group_function' not in args:
    print('NOT ENOUGH ARGUMENT')
    exit()
 
 
if not os.path.exists("state_reload.dat"):
    log.debug('Creating state_reload.dat')
    with open('state_reload.dat', 'w') as fp:
        pass
    log.debug('Created state_reload.dat')
 
 
log.debug('Loading list of group and function')
rel_group = []
rel_function = []
groupfunc = args['list_group_function'].split(";")
log.debug('Loaded list of group and function: ' + str(groupfunc))
for i in range(len(groupfunc)):
    group = (groupfunc[i].split(","))[0]
    function = (groupfunc[i].split(","))[1]
    rel_group.append(group)
    rel_function.append(function)
 
 
log.debug('Loading Excluding Process')
if 'list_processExclude' not in args:
    args['list_processExclude'] = ''
    processExclude = ''
    log.debug('Do not have excluding process')
else:
    if (args['list_processExclude'].lower()).find('rocess') < 0:
        if 'list_IPExclude' not in args:
            args['list_IPExclude'] = args['list_processExclude']
            if (args['list_IPExclude'].lower()).find('rocess') < 0:
                processExclude = ''
                log.debug('Do not have excluding process')
            else:
                if args['list_processExclude'].find(',') >= 0:
                    listprocessExclude = (args['list_processExclude'].split("="))[1]
                    processExclude = listprocessExclude.split(",")
                    processExclude = ','.join("'{0}'".format(process) for process in processExclude)
                else:
                    listprocessExclude = (args['list_processExclude'].split("="))[1]
                    processExclude = listprocessExclude.join("''")
                log.debug('Loaded Excluding Process: ' + str(processExclude))
        else:
            processname_reverse = args['list_IPExclude']
            args['list_IPExclude'] = args['list_processExclude']
            if processname_reverse.find(',') >= 0:
                listprocessExclude = (processname_reverse.split("="))[1]
                processExclude = listprocessExclude.split(",")
                processExclude = ','.join("'{0}'".format(process) for process in processExclude)
            else:
                listprocessExclude = (processname_reverse.split("="))[1]
                processExclude = listprocessExclude.join("''")
            log.debug('Loaded Excluding Process: ' + str(processExclude))
    else:
        if args['list_processExclude'].find(',') >= 0:
            listprocessExclude = (args['list_processExclude'].split("="))[1]
            processExclude = listprocessExclude.split(",")
            processExclude = ','.join("'{0}'".format(process) for process in processExclude)
        else:
            listprocessExclude = (args['list_processExclude'].split("="))[1]
            processExclude = listprocessExclude.join("''")
        log.debug('Loaded Excluding Process: ' + str(processExclude))
 
 
log.debug('Loading Excluding IP')
if 'list_IPExclude' not in args:
    args['list_IPExclude'] = ''
    IPExclude = ''
    log.debug('Do not have excluding IP')
else:
    if (args['list_IPExclude'].lower()).find('pexclude') < 0:
        IPExclude = ''
        log.debug('Do not have excluding IP')
    else:
        if args['list_IPExclude'].find(',') >= 0:
            listIPExclude = (args['list_IPExclude'].split("="))[1]
            IPExclude = listIPExclude.split(",")
            IPExclude = ','.join("'{0}'".format(IP) for IP in IPExclude)
        else:
            listIPExclude = (args['list_IPExclude'].split("="))[1]
            IPExclude = listIPExclude.join("''")
        log.debug('Loaded Excluding IP: ' + str(IPExclude))
 
 
filesize = os.path.getsize("state_reload.dat")
if filesize == 0:
    dictSuccessTime = {}
else:
    with open("state_reload.dat", "r+") as f:
        dictSuccessTime = f.read()
    with open("state_reload.dat", "w+") as f:
        f.writelines(str(dictSuccessTime).replace('}{',','))
    with open("state_reload.dat", "r+") as f:
        dictSuccessTime = ast.literal_eval(str(f.readlines()))
        dictSuccessTime[0] = ast.literal_eval(dictSuccessTime[0])
 
 
log.debug('Loading OAM url sent to reload')
list_urlOAMgroup = []
listallgroup_urlOAMgroup = []
con = cx_Oracle.connect('voam1/vOam1#2016@10.60.144.198:1521/VOAM')
cur = con.cursor()
# fetchall() is used to fetch all records from result set
for i in range(len(rel_group)):
    if IPExclude != '':
        cur.execute("select b.ip_address from voam1.oam_process a, voam1.oam_node_info b, voam1.oam_app c where a.node = b.node_name and a.app_id = c.app_id and c.app_name in ('{0}') and b.ip_address not in ({1})".format(rel_group[i],IPExclude))
    else:
        cur.execute("select b.ip_address from voam1.oam_process a, voam1.oam_node_info b, voam1.oam_app c where a.node = b.node_name and a.app_id = c.app_id and c.app_name in ('{0}')".format(rel_group[i]))
    row_IP = cur.fetchall()
    cur.execute("select to_char(b.monitor_port) from voam1.oam_process a, voam1.oam_node_info b, voam1.oam_app c where a.node = b.node_name and a.app_id = c.app_id and c.app_name in ('{0}')".format(rel_group[i]))
    row_port = cur.fetchall()
    urlOAMgroup_IP = [x for t in row_IP for x in t if isinstance(x, str)]
    urlOAMgroup_port = [x for t in row_port for x in t if isinstance(x, str)]
    urlOAMgroup = ["http://useragent:passagent@" + s + ":" + v for s, v in zip(urlOAMgroup_IP, urlOAMgroup_port)]
    list_urlOAMgroup = list(dict.fromkeys(urlOAMgroup))
    listallgroup_urlOAMgroup.append(list_urlOAMgroup)
log.debug('Loaded OAM url sent to reload: ' + str(listallgroup_urlOAMgroup))
 
 
def reload(server,n,processname):
    dictSuccessTime_reload = {}
    socket.setdefaulttimeout(60)
    res = server.agent.reload(processname, rel_function[n], params)
    if res == 0:
        socket.setdefaulttimeout(None)
        result = 'SUCCESS'
        log.debug('Reload ' + processname + ' of ' +ipport[1] + ' with function ' + rel_function[n] + ': ' + result)
        dictSuccessTime_reload[("{0}|{1}|{2}".format(ipport[1], processname, rel_function[n]))] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with open("state_reload.dat", "a+") as f:
            f.writelines(str(dictSuccessTime_reload))
        with open("state_reload.dat", "r+") as f:
            dictSuccessTime[0] = f.read()
        with open("state_reload.dat", "w+") as f:
            f.writelines(str(dictSuccessTime[0]).replace('}{',','))
    else:
        socket.setdefaulttimeout(None)
        result = 'FAIL'
        count_fail = 0
        while res != 0:
            res = server.agent.reload(processname, rel_function[n], params)
            if count_fail == 5:
                break
            if res != 0:
                log.debug('Reload ' + processname + ' of ' + ipport[1] + ' with function ' + rel_function[n] + ': ' + result + ', code: ' + str(res))
            count_fail += 1
            time.sleep(5)
        if count_fail < 5:
            log.debug('Reload ' + processname + ' of ' + ipport[1] + ' with function ' + rel_function[n] + ': ' + result)
            dictSuccessTime_reload[("{0}|{1}|{2}".format(ipport[1], processname, rel_function[n]))] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            with open("state_reload.dat", "a+") as f:
                f.writelines(str(dictSuccessTime_reload))
            with open("state_reload.dat", "r+") as f:
                dictSuccessTime[0] = f.read()
            with open("state_reload.dat", "w+") as f:
                f.writelines(str(dictSuccessTime[0]).replace('}{', ','))
 
def reload_first(server,n,processname):
    socket.setdefaulttimeout(60)
    res = server.agent.reload(processname, rel_function[n], params)
    if res == 0:
        socket.setdefaulttimeout(None)
        result = 'SUCCESS'
        log.debug('Reload ' + processname + ' of ' +ipport[1] + ' with function ' + rel_function[n] + ': ' + result)
        dictSuccessTime[("{0}|{1}|{2}".format(ipport[1], processname,rel_function[n]))] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with open("state_reload.dat", "a+") as f:
            f.writelines(str(dictSuccessTime))
    else:
        socket.setdefaulttimeout(None)
        result = 'FAIL'
        count_fail = 0
        while res != 0:
            res = server.agent.reload(processname, rel_function[n], params)
            if count_fail == 5:
                break
            if res != 0:
                log.debug('Reload ' + processname + ' of ' + ipport[1] + ' with function ' + rel_function[n] + ': ' + result + ', code: ' + str(res))
            count_fail += 1
            time.sleep(5)
        if count_fail < 5:
            log.debug('Reload ' + processname + ' of ' + ipport[1] + ' with function ' + rel_function[n] + ': ' + result)
            dictSuccessTime[("{0}|{1}|{2}".format(ipport[1], processname, rel_function[n]))] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            with open("state_reload.dat", "a+") as f:
                f.writelines(str(dictSuccessTime))
 
log.debug('Loading process reloaded')
con = cx_Oracle.connect('voam1/vOam1#2016@10.60.144.198:1521/VOAM')
cur = con.cursor()
for i in range(len(rel_group)):
    n = i
    for j in range(len(listallgroup_urlOAMgroup[i])):
        ipport = (listallgroup_urlOAMgroup[i])[j].split('@')
        ipaddr = ipport[1].split(':')
        # fetchall() is used to fetch all records from result set
        if processExclude != '':
            cur.execute("select a.process_name from voam1.oam_process a, voam1.oam_node_info b, voam1.oam_app c where a.node = b.node_name and a.app_id = c.app_id and a.status = 1 and c.app_name in ('{0}') and b.ip_address in ('{1}') and a.process_name not in ({2})".format(rel_group[i],ipaddr[0],processExclude))
        else:
            cur.execute("select a.process_name from voam1.oam_process a, voam1.oam_node_info b, voam1.oam_app c where a.node = b.node_name and a.app_id = c.app_id and a.status = 1 and c.app_name in ('{0}') and b.ip_address in ('{1}')".format(rel_group[i], ipaddr[0]))
        rows = cur.fetchall()
        processnamegroup = [x for t in rows for x in t if isinstance(x,str)]
        log.debug('Loaded processes: ' + str(processnamegroup) + ' from group: ' + str(rel_group[i]) + ' with IP: ' + str(ipaddr[0]))
        processes = []
        for processname in processnamegroup:
            #OAM
            server = xmlrpclib.ServerProxy((listallgroup_urlOAMgroup[i])[j])
            if filesize != 0:
                #for i in range(count):
                try:
                    while True:
                        if dictSuccessTime[0].get("{0}|{1}|{2}".format(ipport[1],processname,rel_function[n])) != None:
                            if (dictSuccessTime[0].get("{0}|{1}|{2}".format(ipport[1], processname, rel_function[n]))).find(datetime.datetime.now().strftime("%d/%m/%Y")) == 0:
                                date_time = (dictSuccessTime[0].get("{0}|{1}|{2}".format(ipport[1], processname, rel_function[n]))).split()
                                if datetime.datetime.strptime(date_time[1],"%H:%M:%S").time() < datetime.datetime.strptime('23:00:00','%H:%M:%S').time():
                                    log.debug("The process {0} with {1} of {2} has been reloaded".format(processname,rel_function[n],ipport[1]))
                                    break
                                else:
                                    with open('state_reload.dat', 'w') as fp:
                                        pass
                                    processname = Process(target=reload, args=(server, n, processname))
                                    processes.append(processname)
                                    break
                            else:
                                processname = Process(target=reload, args=(server, n, processname))
                                processes.append(processname)
                                break
                        else:
                            processname = Process(target=reload, args=(server, n, processname))
                            processes.append(processname)
                            break
                except socket.error:
                    continue
            else:
                try:
                    processname = Process(target=reload_first, args=(server,n,processname))
                    processes.append(processname)
                except socket.error:
                    continue
        for p in processes:
            p.start()
#end

