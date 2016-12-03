def anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten):

    # Update committed log.
    for log_entry in new_log_com:
        if log_entry not in self_log_com:
            self_log_com.append(log_entry)

    # Update tentative log.
    for log_entry in new_log_ten:
        if log_entry not in self_log_ten:
            self_log_ten.append(log_entry)

def primary_anti_entropy(self_log_com, self_log_ten, new_log_com, new_log_ten):

    #Update committed log.
    for log_entry in new_log_ten:
        if log_entry not in self_log_com:
            self_log_com.append(log_entry)
