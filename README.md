
# NanoAOD hadds (skims)

```
python get_info.py -i ../nanocc/metadata/v2BTVx17_local.json -o hashes17b.json -j 80
```
```
python jobs.py -i ../nanocc/metadata/v2x17_local.json -d ~/group/v2nano17merge/ -o ../nanocc/metadata/v2x17_merge.json -s hashes17.json
```