#!/bin/bash

size=10 # In GiB

lfs setstripe -S 2M -c -1 $(hostname).bin
dd if=/dev/urandom of=$(hostname).bin bs=1024M count=$size iflag=fullblock
echo "$?: $size GiB blob done on $(hostname)"
