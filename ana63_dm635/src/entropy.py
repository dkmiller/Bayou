def anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten, new_vv, vv):

    # Update committed log.
    for log_entry in new_log_com:
        if log_entry not in self_log_com:
            if log_entry in self_log_ten:
                self_log_ten.remove(log_entry)
            self_log_com.append(log_entry)

    # Update tentative log.
    for log_entry in new_log_ten:
        if log_entry not in self_log_ten:
            self_log_ten.append(log_entry)

    compute_vv(vv, new_vv)

def primary_anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten, new_vv, vv):

    #Update committed log.
    for log_entry in new_log_ten:
        if log_entry not in self_log_com:
            self_log_com.append(log_entry)

    compute_vv(vv, new_vv)

def compute_vv(vv, new_vv): 
    for server in new_vv:
        if server not in vv:
            vv[server] = new_vv[server]
        else:
            vv[server] = max(vv[server], new_vv[server])
