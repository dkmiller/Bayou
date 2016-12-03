def anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten, vv, new_vv):

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

    compute_vv(new_vv, vv)

def primary_anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten, vv, new_vv):

    #Update committed log.
    for log_entry in new_log_ten:
        if log_entry not in self_log_com:
            self_log_com.append(log_entry)

    compute_vv(new_vv, vv)

def compute_vv(new_vv, vv): 
    for v in vv:
        if v not in new_vv:
            new_vv[v] = vv[v]
        else:
            new_vv[v] = max(new_vv[v], vv[v])
