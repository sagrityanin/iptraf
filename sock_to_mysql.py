import mysql.connector

import datetime
# OPen file
myconn = mysql.connector.connect(host = "localhost", user = "andrey",passwd = "Ctkns", database = "iptraf")
cur = myconn.cursor()
sql = "INSERT INTO ip_log (data_time, protocol, volum, sourse_ip, s_port, destination_ip, d_port) VALUES (%s, %s, %s, %s, %s, %s, %s)"
err = []
err_s_ip = []
date_log = []
with open('/home/andrey/iptraf/iptraf.sock') as f:
#f = open('iptraf.log', 'r')
    for line in f:
        if 'IP traffic monitor started' in line:
            error_file = open('error.log', 'a')
            error_file.write(str(line)+ '\n')
            error_file.close()
            continue
        s_input = line.strip().split()

        d_t = s_input[4].replace(';', '') + ' ' + s_input[1] + ' ' + s_input[2] + ' ' + s_input[3]
        date_time = str(datetime.datetime.strptime(d_t, '%Y %b %d %H:%M:%S'))
        date_log.append(date_time)
        #print(d_t, 'form', date_time)
        protocol = s_input[5].replace(';', '')
        if protocol == 'ICMPv6':
            continue
        if s_input[6] == 'Connection': # check record type
            source_ip, s_port = s_input[7].split(':')
            destination_ip, d_port = s_input[9].replace(';', '').split(':')
            volum = s_input[14]
        elif s_input[5] == 'ICMP':
            source_ip = s_input[10]
            destination_ip = s_input[12]
            d_port = s_input[14]
            volum = s_input[7]
        else:
            volum = s_input[7]
            # print(line)
            # if s_input[10] == 'fe80::a00:27ff:fe96:e0df':
            #     continue
            try:
                source_ip, s_port = s_input[10].split(':')
            except ValueError:
                error_file = open('error.log', 'a')
                error_file.write(str(line) + '\n')
                error_file.close()
                continue
            destination_ip, d_port = s_input[12].replace(';', '').split(':')

        # print(date_time, protocol, volum, source_ip, s_port, destination_ip, d_port)
        val = (date_time, protocol, volum, source_ip, s_port, destination_ip, d_port)
        try:
            # inserting the values into the table
            cur.execute(sql, val)

            # commit the transaction
            myconn.commit()
            # print('suc')

        except:
            error_file = open('error.log', 'a')
            error_file.write(str(line) + '\n')
            error_file.close()
            myconn.rollback()
            # err.append(line)

        #print(cur.rowcount, "record inserted!")

        #print(insert_mysql)

myconn.close()


