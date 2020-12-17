#!/bin/bash
file="${1}"
sed '1s/^/[/;$!s/$/,/;$s/$/]/' "${file}" > "${file}.tmp"
if [ ${PIPESTATUS[1]} != 0 ] ; then
    echo "${file} is broken"
    rm "${file}.tmp"
fi
