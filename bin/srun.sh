#!/bin/bash

size=10 # In GiB

if [[ $1 == "write" ]]; then
	# Set stripe count to max for the bin file
	lfs setstripe -S 2M -c -1 $(hostname).bin
	# Write the bin file striped over all OSTs
	time dd if=/dev/zero of=$(hostname).bin bs=1024M count=$size status=progress 2>> dd.log
	echo "$?: $size GiB blob done on $(hostname)" >> dd.log
elif [[ $1 == "read" ]]; then
	# Read a generic dd file which as been striped over all OSTs
	time dd if=/srv/os3/blub.dd of=/dev/null status=progress 2>> dd.log
	echo "$?: $size GiB blob done on $(hostname)" >> dd.log
else
	echo "Provide argument [read|write]"
	exit 1
fi
