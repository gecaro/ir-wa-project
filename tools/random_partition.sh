# a simple bash script to get a json file and get n random lines from it
# then, it re creates a json file and gzips it
# inputs: the json file, number of lines desired, outputname of the file
file="${1}"
permuts="${2}"
outputname="${3}"
sed -e '1d;$d' "${file}"  > "${file}.tmp"
gshuf -n "${permuts}" "${file}.tmp" -o "${file}_shuf.tmp"
sed -e '$ s/.$//' -e '1s/^/[/;$s/$/]/' "${file}_shuf.tmp" > "${outputname}"
rm "${file}.tmp" 
rm "${file}_shuf.tmp"
gzip "${outputname}"