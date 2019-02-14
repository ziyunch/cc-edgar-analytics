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

def clean_active_log(num):
    """ evoke inactive sessions log and write to file
    """
    global active_log
    global ip_dict
    global f2
    for ip in sorted(active_log[num], key=lambda x: ip_dict[x][7]):
        output_session(ip)

def output_session(ip):
    """ Pop inactive ip and output in file
    """
    global ip_dict
    session_start = ip_dict[ip][0] + " " + ip_dict[ip][1]
    session_end = ip_dict[ip][3] + " " + ip_dict[ip][4]
    duration = str(time_diff(ip_dict[ip][5], ip_dict[ip][2])+1)
    count = str(ip_dict[ip][6])
    output_list = []
    output_list = [ip, session_start, session_end, duration, count]
    output = ",".join(output_list)
    ip_dict.pop(ip)
    f2.write(output+"\n")

def process_weblog(weblog_file, output_file, inact_period):
    """ Process Weblog
    """
    global active_log
    global ip_dict
    global f2
    # Initialize the dictionaries
    ip_dict = {}
    ip_dict[0] = []
    temp = set()
    active_log = [set() for _ in range(0, inact_period)]
    # Read in the weblog file
    with open(weblog_file) as f1, open(output_file, "a") as f2:
        f1.readline()
        # Read in first ip event and initialize first ip session
        line = f1.readline()
        ip, dt, date, time = read_line(line)
        dt0 = dt
        time_count = 0
        event_index = 1
        mod_value = time_count % inact_period
        ip_dict[ip] = [date, time, dt, date, time, dt, 1, event_index]
        temp.add(ip)
        for line in f1:
            # Read in next ip event
            ip, dt, date, time = read_line(line)
            event_index += 1
            diff = time_diff(dt, dt0)
            if diff > time_count:
                # For every second, evoke inactive sessions and write to file
                clean_active_log(mod_value)
                active_log[mod_value] = temp
                if ip in ip_dict:
                    # pop ip from last active session log
                    last_time_count = time_diff(ip_dict[ip][5], dt0)
                    last_mod = last_time_count % inact_period
                    active_log[last_mod].remove(ip)
                time_count = diff
                mod_value = time_count % inact_period
                temp = set()
            # Update ip dictionary for active sessions
            # Add ip in updated current active session log with event index
            if ip not in ip_dict:
                # Initialize ip in this session
                ip_dict[ip] = [date, time, dt, date, time, dt, 1, event_index]
                temp.add(ip)
            else:
                # Update current session's last update time and event count
                ip_dict[ip][3] = date
                ip_dict[ip][4] = time
                ip_dict[ip][5] = dt
                ip_dict[ip][6] += 1
                temp.add(ip)
        # Deal with the remaining ip sessions
        remaindt = set()
        remaindt.update(active_log[mod_value])
        active_log[mod_value] = temp
        for i in range(0, inact_period):
            remaindt.update(active_log[i])
        for ip in sorted(remaindt, key=lambda x: ip_dict[x][7]):
            output_session(ip)

def main():
    if len(sys.argv) != 4:
        print("Four arguments are expected: [window.txt], [actual.txt], [predicted.txt], [comparison.txt]")
        exit(1)
    # Input
    weblog_file = sys.argv[1]
    inact_period_file = sys.argv[2]
    output_file = sys.argv[3]
    # Read window value from `inactivity_period_file`, O(1)
    inact_period = int(open(inact_period_file).read())
    # Process weblog
    process_weblog(weblog_file, output_file, inact_period)

if __name__ == "__main__":
    main()