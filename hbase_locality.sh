#! /bin/bash
#

THRESHOLD=2
IGNORE_REGION='hbase:meta,hbase:acl,hbase:namespace'

echo 'scan "hbase:meta"' | hbase shell >/tmp/hbase_meta.txt

IGNORE_REGION=$(echo $IGNORE_REGION | sed 's/[ ,]/|/g')
sed -n '/ column=info:server,/s/^[ \t]*\([^,]*\),[^,]*,[^.]*\.\([^.]*\)\..*value=\([^:]*\):[0-9]*$/\1 \2 \3/p' /tmp/hbase_meta.txt | grep -vE "$IGNORE_REGION" | sort >/tmp/hbase_region.txt

hdfs fsck /hbase/data/default/ -blocks -files -racks >/tmp/hbase_block.txt

: >/tmp/locality.txt

while read table region hostname
do
    ip=$(host $hostname | grep -oP '(\d+\.){3}\d+$')
    if [[ -z $ip ]]
    then
        echo "get ip failed: $table $region $hostname"
        continue
    fi

    locality=$(awk 'BEGIN {
        STATE = "INIT";
        N_BLOCK = 0;
        N_LOCALITYBLOCK = 0;
    } STATE == "INIT" && $0 ~ /^\/hbase\/data\/default\/'"$table"'\/'"$region"' <dir>$/ {
        STATE = "IN_RG";
        next;
    } STATE == "IN_RG" && $0 ~ /^\/hbase\/data\/default\/'"$table"'\/'"$region"'\/[^ \t]+ [0-9]+ bytes, [0-9]+ block\(s\):/ && $0 !~ /^\/hbase\/data\/default\/'"$table"'\/'"$region"'\/.regioninfo [0-9]+ bytes, [0-9]+ block\(s\):/{
        STATE = "IN_HF";
        next;
    } STATE == "IN_HF" && $0 ~ /^[0-9]+. BP-/ {
        N_BLOCK += 1;
        if ($0 ~ /\/'"$ip"':/) {
            N_LOCALITYBLOCK += 1;
        }
        next;
    } STATE == "IN_HF" && $0 ~ /^$/ {
        STATE = "IN_RG"
        next;
    } STATE == "IN_RG" && $0 ~ /^\/hbase\/data\/default\/[^/]+\/[0-9a-z]+ <dir>$/ && $0 !~ /^\/hbase\/data\/default\/'"$table"'\/'"$region"' <dir>$/{
        if (N_BLOCK == 0) {
            printf("%0.2f\n", 1);
        } else {
            printf("%0.2f\n", N_LOCALITYBLOCK/N_BLOCK);
        }
        exit;
    } STATE == "IN_RG" && $0 ~ /^Status: HEALTHY/{
        if (N_BLOCK == 0) {
            printf("%0.2f\n", 1);
        } else {
            printf("%0.2f\n", N_LOCALITYBLOCK/N_BLOCK);
        }
        exit;
    }' /tmp/hbase_block.txt)

    if [[ -z $locality ]]
    then
        echo "locality is null: $table $region $hostname"
        continue
    fi

    if [[ $(echo "$locality < $THRESHOLD" | bc) -eq 1 ]]
    then
        echo "$table $region $hostname $locality" | tee -a /tmp/locality.txt
    fi
done </tmp/hbase_region.txt

awk 'NF==4{s += $4; next;}{print "NF != 4";}END{printf("%0.2f\n", s/FNR)}' /tmp/locality.txt

awk 'NF==4{s[$1] += $4; n[$1] += 1; next;}{print "NF != 4"}END{for(i in s){printf("%s %0.2f\n", i, s[i]/n[i])}}' /tmp/locality.txt

