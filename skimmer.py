#!/usr/bin/env python
import os
import sys
import ast
import json
import time
import argparse
from pathlib import Path

import parsl
from parsl import bash_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

def sizeof_fmt(num, suffix=''):
    """Readable file size
    :param num: Bytes value
    :type num: int
    :param suffix: Unit suffix (optionnal) default = o
    :type suffix: str
    :rtype: str
    """
    for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample', usage="./SkimTrees.py sample_name")
    parser.add_argument('-i', '--input', default=r'dummy.json', help='')
    parser.add_argument('-d', '--dir', help='Output directory', required=True)
    parser.add_argument('-o', '--output', default=r'dummy_skim.json', help='')
    parser.add_argument('--limit', type=int, default=None, help='')
    parser.add_argument('-m', type=int, default=2000,
                        help="MB size of input files to merge")
    parser.add_argument('-j', '--ncpu', type=int, default=1,
                        help="Number of CPUs to use with RDataFrame")
    parser.add_argument('--run', action='store_true', help='Bool')
    args = parser.parse_args()

    start = time.time()

    with open(args.input) as f:
        sample_dict = json.load(f)

    # Hadd instance
    @bash_app
    def skim_files(filelist=[], out='hadded.root', ith=None, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
        if ith is not None:
            print(ith)
        return f'python -b mergeskim.py {out} {" ".join(filelist)}'


    print("Storage dir:")
    print("   ", os.path.abspath(args.dir))

    # Setup multithreading
    config = Config(executors=[ThreadPoolExecutor(max_threads=args.ncpu)])
    parsl.load(config)

    # Write futures
    out_dict = {} # Output filename list
    run_futures = {} # Future list
    for key in sorted(sample_dict.keys()):
        run_futures[key] = []
        # Make size batches
        batches = []
        batch_list = []
        batch_size, tot_size = 0, 0
        for i, fname in enumerate(sample_dict[key]):
            batch_size += os.stat(fname).st_size
            batch_list.append(fname)
            # batch end conditions:
            _batch_size = batch_size > args.m*1024*1024
            _batch_end = i == len(sample_dict[key]) - 1
            _batch_run_boundary = False
            if len(batch_list) > 0 and not _batch_end:
                _batch_run_boundary = fname.split("/Run20")[-1][:3] != sample_dict[key][i+1].split("/Run20")[-1][:3]
            if _batch_size or _batch_end or _batch_run_boundary:
                batches.append(batch_list)
                tot_size += batch_size
                batch_size = 0
                batch_list = []

        # print(key, len(sample_dict[key]), len(batches), len(sum(batches, [])))
        if len(sample_dict[key]) != len(sum(batches, [])):
            raise ValueError('Number of inputs files is not the same as batched total.')
        for i, fnames in enumerate(batches):
            if len(fnames) == 0:
                continue
            if i%5 == 0: 
                # generate some progress strings
                ith = f'{key}: {i}/{len(batches)} ({sizeof_fmt(tot_size)})'
            else:
                ith = None
            outpath = os.path.join(os.path.abspath(args.dir), key.strip('/'))
            Path(outpath).mkdir(parents=True, exist_ok=True)
            out = os.path.join(outpath, fnames[0].split("/")[-1].lstrip("/"))
            x = skim_files(filelist=fnames, out=out, ith=ith)
            run_futures[key].append(x)
        

    if args.run:
        for key, future_list in run_futures.items():
            new_list = []
            for i, r in enumerate(future_list):
                r.result()
                with open(r.stdout, 'r') as f:
                    new_list.extend(ast.literal_eval(f.read()))
            out_dict[key] = new_list
        

    print("Writing files to {}".format(args.output))
    with open(args.output, 'w') as fp:
        json.dump(out_dict, fp, indent=4)

    end = time.time()
    print("TIME:", time.strftime("%H:%M:%S", time.gmtime(end-start)))
