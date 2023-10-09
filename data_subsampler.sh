cd /my/binanceapi

d="2023-02-08"
until [[ $d > 2023-02-09 ]]; do
    date=$d
    echo "$date"
    d=$(date -I -d "$d + 1 day")
    if [ '' -a -f /my/notebooks/spot_${date}.parquet ] ; then
        echo $date already done
        continue
    fi
    python3 data_subsampler.py --fr=$date --step=5 $@
done



