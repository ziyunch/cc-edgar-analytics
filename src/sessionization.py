import sys
import datetime

def read_line(line):
    """ Process each line
    """
    csv_row = line.split(",")
    ip = csv_row[0]
    date = csv_row[1]
    time = csv_row[2]
    datetime_str = date + " " + time
    format_str = "%Y-%m-%d %H:%M:%S"
    datetime_obj = datetime.datetime.strptime(datetime_str, format_str)
    return ip, datetime_obj, date, time

def time_diff(dt, dt0):
    """ Calculate difference between two datetime object and return in seconds
    """
    diff_obj = dt - dt0
    diff = int(diff_obj.total_seconds())
    return diff

def clean_active_log(log_file, ip_dict, f2):
    """ evoke inactive sessions log and write to file
    """
    for ip in sorted(log_file, key=lambda x: ip_dict[x][3]):
        ip_dict = output_session(ip, ip_dict, f2)
    return ip_dict

def output_session(ip, ip_dict, f2):
    """ Pop inactive ip and output in file
    """
    session_start = ip_dict[ip][0][0] + " " + ip_dict[ip][0][1]
    session_end = ip_dict[ip][1][0] + " " + ip_dict[ip][1][1]
    duration = str(time_diff(ip_dict[ip][1][2], ip_dict[ip][0][2])+1)
    count = str(ip_dict[ip][2])
    output_list = []
    output_list = [ip, session_start, session_end, duration, count]
    output = ",".join(output_list)
    ip_dict.pop(ip)
    f2.write(output+"\n")
    return ip_dict

def update_ip(ip_dict, pair, temp, mod_value, event_index):
    """Update ip dictionary for active sessions
       Add ip in updated current active session log with event index
    """
    ip, dt, date, time = pair
    if ip not in ip_dict:
        # Initialize ip in this session
        ip_dict[ip] = [[date, time, dt], [date, time, dt, mod_value], 1, event_index]
        temp.add(ip)
    else:
        # Update current session's last update time and event count
        ip_dict[ip][1] = [date, time, dt, mod_value]
        ip_dict[ip][2] += 1
        temp.add(ip)
    return ip_dict, temp

def process_weblog(f1, f2, ip_dict, temp, active_log, inact_period):
    """ Process Weblog
    """
    f1.readline()
    # Read in first ip event and initialize first ip session
    line = f1.readline()
    ip, dt, date, time = read_line(line)
    dt0 = dt
    time_count = 0
    event_index = 1
    mod_value = time_count % inact_period
    ip_dict[ip] = [[date, time, dt], [date, time, dt, 0], 1, event_index]
    temp.add(ip)
    for line in f1:
        # Read in next ip event
        pair = read_line(line)
        ip, dt, date, time = pair
        event_index += 1
        diff = time_diff(dt, dt0)
        if diff > time_count:
            # For every second, evoke inactive sessions and write to file
            ip_dict = clean_active_log(active_log[mod_value], ip_dict, f2)
            active_log[mod_value] = temp
            if ip in ip_dict:
                # pop ip from last active session log
                active_log[ip_dict[ip][1][3]].remove(ip)
            time_count = diff
            mod_value = time_count % inact_period
            temp = set()
        ip_dict, temp = update_ip(ip_dict, pair, temp, mod_value, event_index)
    # Deal with the remaining ip sessions
    remaindt = set()
    remaindt.update(active_log[mod_value])
    active_log[mod_value] = temp
    for i in range(0, inact_period):
        remaindt.update(active_log[i])
    ip_dict = clean_active_log(remaindt, ip_dict, f2)

def main():
    if len(sys.argv) != 4:
        print("Three arguments are expected")
        exit(1)
    # Input
    weblog_file = sys.argv[1]
    inact_period_file = sys.argv[2]
    output_file = sys.argv[3]
    # Read window value from `inactivity_period_file`, O(1)
    inact_period = int(open(inact_period_file).read())
    # Initialize the dictionaries
    ip_dict = {}
    ip_dict[0] = []
    temp = set()
    active_log = [set() for _ in range(0, inact_period)]
    # Process the weblog file
    with open(weblog_file) as f1, open(output_file, "a") as f2:
        process_weblog(f1, f2, ip_dict, temp, active_log, inact_period)

if __name__ == "__main__":
    main()