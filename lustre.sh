#!/bin/bash

declare -A mounts pids
declare mount_path mtab


mounts=(["ost/1"]="sdb" ["ost/2"]="sdc" ["ost/3"]="sdd" ["ost/4"]="sde" ["ost/5"]="sdf" ["ost/6"]="sdg"
        ["mdt"]="sdh")
mtab=($(< /etc/mtab))
mount_option=$1
if [[ -z $2 ]]; then
	mount_path="/srv/os3"
else
	mount_path=$2
fi


function help() {
  if [[ -z $mount_option || $mount_option == '--help' || $mount_option == '-h' ]]; then
    echo -e "${0##*/} options [mount point]\nExample: ${0##*/} mount /mnt/lustre\n\nOptions:\n  mount\t\tMount Lustre\n  umount\tUnmount Lustre"
  else
    echo "$mount_option is invalid, check ${0##*/} --help"
  fi
  exit 1
}


for key in ${!mounts[@]}; do
  if [[ $mount_option == "mount" ]]; then
    if [[ ${mtab[@]/$key} == ${mtab[@]} ]]; then
      echo "Mounting /dev/${mounts[$key]} on $mount_path/$key..."
      mount -t lustre /dev/${mounts[$key]} $mount_path/$key 2>/dev/null &
      pids[$key]=$!
    else
      echo "Mounting /dev/${mounts[$key]} on /mnt/scratch/$key... Already mounted"
    fi
  elif [[ $mount_option == "umount" ]]; then
    if [[ ${mtab[@]/$key} != ${mtab[@]} ]]; then
      echo "Unmounting $mount_path/$key..."
      umount $mount_path/$key 2>/dev/null &
      pids[$key]=$!
    else
      echo "Unmounting $mount_path/$key... Already unmounted"
    fi
  else
    help
    break
  fi
done


for key in ${!pids[@]}; do
  echo "Waiting for background processes ${pids[$key]} ($key) to complete..."
  wait ${pids[$key]}
  if [[ $? != 0 ]]; then
    echo "$key gave an error at exit"
  fi
done
echo "Done"
