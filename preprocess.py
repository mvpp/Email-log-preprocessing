import csv
import time
from datetime import datetime, timedelta
import mmap
import itertools
from collections import defaultdict

csv.field_size_limit(1000000000)

email_wd = "/Users/liuxihan/Google Drive/percipiomedia/emails.tsv"
response_wd = "/Users/liuxihan/Google Drive/percipiomedia/email_responses.tsv"
member_wd = "/Users/liuxihan/Google Drive/percipiomedia/members.tsv"


# Explotary stats on overall level
def rates( response_wd ):
	with open(response_wd, "r") as responsefile:

		mapInput_re = mmap.mmap(responsefile.fileno(), 0, prot=mmap.PROT_READ)
		opens = defaultdict(int)
		unique_opens = defaultdict(int)
		clicks = defaultdict(int)
		unique_clicks = defaultdict(int)
		unsubscribes = defaultdict(int)
		open_clicks = defaultdict(int)
		# Hash response content via standard file methods
		for s in iter(mapInput_re.readline, ""):
			key = s.split('\t')[0]
			if s.split('\t')[2].strip() == 'open':
				opens[key] += 1
				unique_opens[key] = 1
			if s.split('\t')[2].strip() == 'click':
				clicks[key] += 1
				unique_clicks[key] = 1
			if s.split('\t')[2].strip() == 'unsub':
				unsubscribes[key] = 1
		mapInput_re.close()

	for k in unique_opens.iterkeys():
		if k in unique_clicks:
			open_clicks[k] = 1

	open_rate = 100.0 * sum(opens[k] for k in opens.iterkeys()) / email_count
	unique_open_rate = 100.0 * sum(unique_opens[k] for k in unique_opens.iterkeys()) / email_count
	click_rate = 100.0 * sum(clicks[k] for k in clicks.iterkeys()) / email_count
	unique_click_rate = 100.0 * sum(unique_clicks[k] for k in unique_clicks.iterkeys()) / email_count
	unsubscribe_rate = 100.0 * sum(unsubscribes[k] for k in unsubscribes.iterkeys()) / email_count
	open_click_rate = 100.0 * sum(open_clicks[k] for k in open_clicks.iterkeys()) / email_count
	click_rate_given_open = 100.0 * open_click_rate / unique_open_rate

	print "Open rate =", "%0.2f" % open_rate, "%"
	print "Unique open rate =", "%0.2f" % unique_open_rate, "%"
	print "Click-through rate =", "%0.2f" % click_rate, "%"
	print "Unique click-through rate =", "%0.2f" % unique_click_rate, "%"
	print "Unsubscribe rate =", "%0.2f" % unsubscribe_rate, "%"
	print "P(Open & Click) = ", "%0.2f" % open_click_rate, "%"
	print "P(Click|Open) = ", "%0.2f" % click_rate_given_open, "%"
	return open_rate, unique_open_rate, click_rate, unique_click_rate, \
	unsubscribe_rate, open_click_rate, click_rate_given_open, 


def proc_email(email_wd, response_wd):
	start_time = time.time()
	output = open('procemails.tsv', 'w') 
	writer = csv.writer(output, delimiter= '\t', lineterminator = '\n')
	# Attribute names: 
	writer.writerow(['email_id', 'time', 'type', 'member_id', 'opened', 'clicked','unsubscribed', 'first_click'])
	output.close()

	with open(email_wd, "r") as emailfile, open(response_wd, "r") as responsefile, \
	open("procemails.tsv", "a+") as output:
		email_record = csv.reader(emailfile, delimiter='	')

		writer = csv.writer(output, delimiter= '\t', lineterminator = '\n')

		d = defaultdict(list)
		first_click = defaultdict(str)
		# memory-mapInput the file, size 0 means whole file
		mapInput_re = mmap.mmap(responsefile.fileno(), 0, prot=mmap.PROT_READ)

		# Muiti-hash response content via standard file methods
		for s in iter(mapInput_re.readline, ""):
			key = s.split('\t')[0]
			re_time = s.split('\t')[1]
			response = s.split('\t')[2].strip()
			d[key].append(response)

			if response == 'click':
				if key in first_click:
					continue
				first_click[key] = re_time

		action = dict((k, tuple(v)) for k, v in d.iteritems())
		mapInput_re.close()
			
		email_count = 0
		# iterate the whole email diary
		for email_line in email_record:
			email_count += 1
			entry = []
			opened = 0
			clicked = 0
			unsubscribed = 0
			entry.append(email_line[0].encode('utf-8'))
			entry.append(email_line[1].encode('utf-8'))
			etype = email_line[3].encode('utf-8').split('_')
			if etype[0] == 'account':
				entry.append('welcome')
			elif etype[0] == 'job':
				entry.append('jobAlert')
			elif etype[0] == 'fixed':
				entry.append('keywords')
			else:
				entry.append('forgotPassword')
			entry.append(email_line[4].encode('utf-8'))
			
			if email_line[0] in action:
				opened = action[email_line[0]].count('open')
				clicked = action[email_line[0]].count('click')
				unsubscribed = action[email_line[0]].count('unsubscribed')

			entry.append(opened)
			entry.append(clicked)
			entry.append(unsubscribed)

			if email_line[0] in first_click:
				entry.append(first_click[email_line[0]])
			else:
				entry.append('')

			writer.writerow(entry)
		
	print "Total number of emails sent:", email_count		
	print "Wrote to tsv"
	print time.time() - start_time, "seconds in total"
	return email_count


def avg_time(timelist):
	tstr = "%Y-%m-%d %H:%M:%S"
	n = 0
	sumTime = timedelta(0)
	for t in timelist:
		sumTime += timedelta(hours = datetime.strptime(t, tstr).hour, \
			minutes = datetime.strptime(t, tstr).minute, \
			seconds = datetime.strptime(t, tstr).second)
		n += 1
	avgTime = sumTime / n
	return (datetime.min + avgTime).time()

def cleaner(member_wd):
	with open(member_wd, "r") as memberfile, open("clean_members.tsv", "w") as output:
		writer = csv.writer(output, delimiter= '\t', lineterminator = '\n')
		for line in memberfile:
			line = lline.replace('\"', '').replace('\\','').replace('\n','')
			writer.writerow(line.split('\t'))
	print "Cleaned"

def proc_member(email_wd, response_wd, member_wd):
	start_time = time.time()
	output = open('procmembers.tsv', 'w') 
	writer = csv.writer(output, delimiter= '\t', lineterminator = '\n')
	# Attribute names: 
	writer.writerow(['member_id', 'join_date', 'email_domain', 'state', 'zip', \
	'degree', 'hs_or_ged_year', 'welcome_received', 'forgot_password_received', \
	'keyword_received', 'job_alert_received', 'most_recent_click', 'opens', \
	'clicks', 'unsubs', 'avg_send_time', 'avg_open_time', 'avg_click_time'])
	output.close()

	with open(email_wd, "r") as emailfile, open(response_wd, "r") as responsefile, \
	open(member_wd, "r") as memberfile, open("procmembers.tsv", "a+") as output:
		mapInput_em = mmap.mmap(emailfile.fileno(), 0, prot=mmap.PROT_READ)
		welcome_received = defaultdict(int)
		forgot_password_received = defaultdict(int)
		keyword_received = defaultdict(int)
		job_alert_received = defaultdict(int)
		emails = defaultdict(str)
		send_time = defaultdict(list)

		for s in iter(mapInput_em.readline, ""):
			emails[s.split('\t')[0]] = s.split('\t')[4].strip()

			key = s.split('\t')[4].strip()
			if s.split('\t')[3].split('_')[0] == 'account':
				welcome_received[key] += 1
			if s.split('\t')[3].split('_')[0] == 'job':
				job_alert_received[key] += 1
			if s.split('\t')[3].split('_')[0] == 'fixed':
				keyword_received[key] += 1
			else:
				forgot_password_received[key] += 1

			send_time[key].append(s.split('\t')[1])
		mapInput_em.close()

		mapInput_re = mmap.mmap(responsefile.fileno(), 0, prot=mmap.PROT_READ)
		most_recenct_click = defaultdict(str)
		opens = defaultdict(int)
		clicks = defaultdict(int)
		unsubs = defaultdict(int)
		open_time = defaultdict(list)
		click_time = defaultdict(list)
		for s in iter(mapInput_re.readline, ""):
			kk = s.split('\t')[0]
			if s.split('\t')[2].strip() == 'open':
				opens[emails[kk]] += 1
				open_time[emails[kk]].append(s.split('\t')[1])
			if s.split('\t')[2].strip() == 'click':
				most_recenct_click[emails[kk]] = s.split('\t')[1]
				clicks[emails[kk]] += 1
				click_time[emails[kk]].append(s.split('\t')[1])
			if s.split('\t')[2].strip() == 'unsub':
				unsubs[emails[kk]] += 1
		mapInput_re.close()

		writer = csv.writer(output, delimiter= '\t', lineterminator = '\n')
		member_record = csv.reader(memberfile, delimiter='	')
		member_record.next()
		member_count = 0
		for m in member_record:
			member_count += 1
			entry = []
			entry.append(m[0].strip().encode('utf-8'))
			entry.append(m[1].strip().encode('utf-8'))
			entry.append(m[2].strip().lower().encode('utf-8'))
			entry.append(m[5].strip().upper().encode('utf-8'))
			entry.append(m[6].strip().encode('utf-8'))
			entry.append(m[7].strip().encode('utf-8'))
			entry.append(m[8].strip().encode('utf-8'))
			if m[0] in welcome_received:
				entry.append(welcome_received[m[0]])
			else:
				entry.append(0)
			if m[0] in forgot_password_received:
				entry.append(forgot_password_received[m[0]])
			else:
				entry.append(0)
			if m[0] in keyword_received:
				entry.append(keyword_received[m[0]])
			else:
				entry.append(0)
			if m[0] in job_alert_received:
				entry.append(job_alert_received[m[0]])
			else:
				entry.append(0)
			if m[0] in most_recenct_click:
				entry.append(most_recenct_click[m[0]])
			else:
				entry.append('')
			if m[0] in opens:
				entry.append(opens[m[0]])
			else:
				entry.append(0)
			if m[0] in clicks:
				entry.append(clicks[m[0]])
			else:
				entry.append(0)
			if m[0] in unsubs:
				entry.append(unsubs[m[0]])
			else:
				entry.append(0)
			if m[0] in send_time:
				entry.append(avg_time(send_time[m[0]]))
			else: 
				entry.append('')
			if m[0] in open_time:
				entry.append(avg_time(open_time[m[0]]))
			else: 
				entry.append('')
			if m[0] in click_time:
				entry.append(avg_time(click_time[m[0]]))
			else: 
				entry.append('')
			writer.writerow(entry)
	print "Total number of members:", member_count
	print "Wrote to tsv"
	print time.time() - start_time, "seconds in total"
	return member_count

		
def why_not_open(procmember_wd):
	with open(procmember_wd, "r") as procmember:
		mapInput_procm = mmap.mmap(procmember.fileno(), 0, prot=mmap.PROT_READ)
		email_domain = defaultdict(int)
		email_domain_all = defaultdict(int)
		for s in iter(mapInput_procm.readline, ""):
			key = s.split(',')[2].lower()
			email_domain_all[key] += 1
			if s.split(',')[12] == '0' and s.split(',')[13] != '0':
				email_domain[key] += 1
	mapInput_procm.close()
	lift = defaultdict(float)
	for k in sorted(email_domain, key=email_domain.get, reverse=True):
		lift[k] = 100.0 * email_domain[k] / email_domain_all[k]
  		print k, "=", "%0.2f" % lift[k], "%"
	return lift




proc_email(email_wd, response_wd)
procemail_wd = "/Users/liuxihan/Google Drive/percipiomedia/procemails.csv"
cleaner(member_wd)
clean_member_wd = "/Users/liuxihan/Google Drive/percipiomedia/clean_members.tsv"
proc_member(email_wd, response_wd, clean_member_wd)
procmember_wd = "/Users/liuxihan/Google Drive/percipiomedia/procmembers.csv"
why_not_open(procmember_wd)
rates(response_wd)

