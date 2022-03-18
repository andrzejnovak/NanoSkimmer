
# NanoAOD hadd/skims

NanoAOD has unfortunate property of not recording branches that were not filled in
production. So files from the same production don't necessarily have the same branches
and cannot be hadded in a straightforward manner.

#### Generate per file branch list hashes to match files.
```
python get_info.py -i dummy.json -o hashes/dummy.json -j 80
```

## Hadding
Uses parsl (local or slurm) to scale out jobs. Actual logic is implemented in : 
- `haddnano.py` - standard `haddnano` script
- `modhaddnano.py` - modifed `haddnano` script, that can drop branches
- `check_hadd.py` - counts number of events before and after


##### Prepare jobs
```
python jobs.py -i jsons_in/dummy.json -d ~/group/storage/dummy_merge/ -o jsons_out/dummy_merge.json -s hashes/dummy.json -j 20
```
Will compute the input mergemap and generate `jsons_out/dummy_merge.json` and
`jsons_out/dummy_merge_log.json`, containing the output json and the mergemap
respectively.

##### Run merge jobs
```
python jobs.py -i jsons_in/dummy.json -d ~/group/storage/dummy_merge/ -o jsons_out/dummy_merge.json -s hashes/dummy.json -j 20 --run
```
Will submit `haddnano.py` jobs. 

##### Run skim jobs
```
python jobs.py -i jsons_in/dummy.json -d ~/group/storage/dummy_skim/ -o jsons_out/dummy_skim.json -s hashes/dummy.json -j 20 --run --skim --branches branches.json
```
Will submit `modhaddnano.py` jobs, dropping the branches in `branches.json`.

##### Check outputs
```
python jobs.py -i jsons_in/dummy.json -d ~/group/storage/dummy_skim/ -o jsons_out/dummy_skim.json -s hashes/dummy.json -j 20 --run --check
```
Will submit `check_hadd.py` and report the number of events in/out.

## Scale out

Run with `--parsl -j 4`. `-j` denotes number of nodes (all cores on node are used), so
use a lower number.

## Todo
- Clean up `haddnano.py` python with argparse/__main__/etc...
- Add compression as argument
