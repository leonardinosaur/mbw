"""
mbw - An augmentation/a wrapper for Hauke Bartsch's MagickBox
		(https://github.com/HaukeBartsch/MagickBox)
		(https://github.com/HaukeBartsch/mb)

Adds ability to manage multiple projects that are using the MagickBox for processing

mbw commands
============

make: Make a new project in ~/mb_projects

curr: Check which current project we are working on

send: Send scans to MagickBox for processing

unpack: Unzip results from MagickBox processing

status: Get count of processes of interest and count of
	how many of those have finished processing

help: Print help


Examples
========


To-do
=====

Use argparse maybe
mb pull
queue and monitor functionality
cant assume mb lives in home directory


"""

import os
import sys
from glob import glob
import random
import shutil
from datetime import datetime

mbp_dir = os.environ['MBPROJ_DIR']
if mbp_dir.endswith('/'):
	mbp_dir = mbp_dir[:-1]

def rand_id():
	alpha_num = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
	return ''.join(random.choice(alpha_num) for i in range(16))


def read_csv(fname):
	out_df = {'PatientID' : [], 'Screen': [], 'Follow': []}
	with open(fname, 'r') as r:
		lines = r.readlines()
		for line in lines:
			vals = line.split(',')
			out_df['PatientID'].append(vals[0].replace('\n', ''))
			out_df['Screen'].append(vals[1].replace('\n', ''))
			out_df['Follow'].append(vals[2].replace('\n', ''))
	return out_df

def get_proj_dir():
        curr_proj = ''
        with open('%s/project.txt' % (mbp_dir), 'r') as r:
                curr_proj = r.readlines()[0].replace('\n', '')
        proj_dir = '%s/' % mbp_dir + curr_proj
	return proj_dir


if len(sys.argv) == 1:
	print 'No command supplied'
	print __doc__
	sys.exit()

cmd = sys.argv[1]

if cmd == 'help':
	print __doc__

elif cmd == 'curr':
	print get_proj_dir().replace('%s/' % (mbp_dir), '').replace('/', '')

elif cmd == 'switch':
	new_proj = sys.argv[2]
	if not os.path.exists('%s/%s' % (mbp_dir, new_proj)):
		sys.exit('Error: Project %s does not exist; please create the project or try switching to a different one')
	with open('%s/project.txt' % (mbp_dir), 'w') as w:
		w.write(new_proj)

elif cmd == 'make':
	proj_name, aets = sys.argv[2], sys.argv[3:]
	if os.path.exists('%s/%s' % (mbp_dir, proj_name)):
		print 'Project already exists; please provide different project name'
	else:
		for aet in aets:
			os.system('mkdir -p %s/%s/%s_inputs' % (mbp_dir, proj_name, aet))
			os.system('mkdir -p %s/%s/%s_outputs' % (mbp_dir, proj_name, aet))
		os.system('mkdir -p %s/%s/logs' % (mbp_dir, proj_name))
		with open('%s/project.txt' % (mbp_dir), 'w') as w:
			w.write(sys.argv[2])

elif cmd == 'add':
	print 'Not implemented'

elif cmd == 'status':
	proj_dir = get_proj_dir()
	scratch = sys.argv[2]
	logfname = proj_dir + '/logs/tmp.status.' + rand_id() + '.txt'
	cmd = '~/mb list %s > %s' % (scratch, logfname) 
	print 'Finding MagickBox processes that match: ', scratch
	os.system(cmd)
	num_matches, num_procs = 0, 0
	with open(logfname, 'r') as r:
		lines = r.readlines()
		curr_caller = -4
		for line in lines:
			if 'AETitleCaller' in line:
				num_matches += 1
				curr_caller = line.replace('\n', '')
			if 'lastChangedTime' in line:
				if ": \"0\"," in line:
					continue
				else:
					print curr_caller, line.replace('\n', '')
					num_procs += 1
	print "Number of processes started/completed: ", num_procs
	print "Number of matches: ", num_matches
	print "Full mb list output is saved at: ", logfname

elif cmd == 'send':
	proj_dir = get_proj_dir()
	aet = sys.argv[2]

	send_limit = 500
	if len(sys.argv) > 3:
		send_limit = float(sys.argv[3])
	in_dir = proj_dir + '/' + aet + '_inputs'

	# Check if this stream has been added
	if not os.path.exists(in_dir):
		sys.exit('Error: Directory for %s stream inputs does not exist' % (aet))


	# Get path to inputs
	all_inps = sorted(glob(in_dir + '/*/'))
	num_sent = 0
	for inp in all_inps:
		
		# Check if we have surpassed the limit
		# of directories we can send
		if num_sent >= send_limit:
			break


		print 'Found input directory', inp
		if os.path.exists(inp + '/progress.txt'):
			print "Directory already sent for processing - skipping"
			print ''
			continue
		
		# Set sender
		cmd = '~/mb w mbw_sender_%s_%s' % (aet, inp.replace(in_dir, '').replace('/', ''))
		print cmd
		os.system(cmd)
		
		# Send files
		cmd = '~/mb push %s %s' % (aet, inp)
		print cmd
		os.system(cmd)

		# Increase send count
		num_sent += 1

		# Write progress file
		print ''
		stamp_fname = inp + '/progress.txt'
		with open(stamp_fname, 'w') as w:
			curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			w.write('Sent to MagickBox stream %s at %s' % (aet, curr_time))

elif cmd == 'retrieve':
	sys.exit('Not yet implemented')
	# Get list of files to pull


	# Omit the ones that are already in the STREAM_outputs directory




elif cmd == 'unpack':
	sys.exit('Not yet implemented')
	# 
	
else:
	print 'Command %s not recognized' % cmd
