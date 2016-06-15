#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python
import subprocess
import re
import sys


base_path = "hdfs://discovery3:9000/"
userPattern = re.compile('- ([a-zA-z]+\.?[a-zA-z]+)')


def get_users(folder):
	s = subprocess.check_output(['hadoop', 'fs', '-ls', base_path + folder])
    	return parse_output(s)


def parse_output(s):
	users = []
	sp = s.split('\n')
	for line in sp:
		if line.startswith('d'):
			users.append(userPattern.search(line).group(1))

	return users


def usage(users, folder):
	usage = {}
	for user in users:
		usage[user] = tb(get_usage(user, folder))

	return usage


def get_usage(user, folder):
	try:
		op = subprocess.check_output(['hadoop', 'fs', '-du', '-s', base_path + folder + '/' + user])
	except:
		return 0
	return parse_usage(op)


def parse_usage(op):
	sp = op.split('\n')
	return sp[2].split()[0]				


def tb(bytes):
	return int(bytes) * (10 ** -12)


def write_to_file(usage, name):
	print "Writing user HDFS usage in file : %s" %name
	with open(name, 'w') as f:
		for user, tb in usage.iteritems():
			f.write('{}\t{}\n'.format(user, tb))



if __name__ == '__main__':
	if len(sys.argv) == 2:
		folder = sys.argv[1]
	 	users = get_users(folder)
		usage = usage(users, folder)
		write_to_file(usage, folder)

	else:
		print "Usage: ./usage <folder_name>"


