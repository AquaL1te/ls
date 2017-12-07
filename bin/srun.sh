#!/bin/bash

size=10 # In GiB

if [[ $1 == "write" ]]; then
	lfs setstripe -S 2M -c -1 $(hostname).bin
	time dd if=/dev/zero of=$(hostname).bin bs=1024M count=$size status=progress 2>> dd.log
	echo "$?: $size GiB blob done on $(hostname)" >> dd.log
elif [[ $1 == "read" ]]; then
	time dd if=/srv/os3/blub.dd of=/dev/null status=progress 2>> dd.log
	echo "$?: $size GiB blob done on $(hostname)" >> dd.log
fi
