#!/usr/bin/env python

from pathlib import Path
import argparse
import sys
import os
import hashlib
import ROOT
ROOT.gROOT.SetBatch()
ROOT.gErrorIgnoreLevel = ROOT.kError
import time
import json
import parsl
import pandas as pd
from parsl.app.app import python_app, bash_app
from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
from parsl.channels import LocalChannel
from parsl.executors.threads import ThreadPoolExecutor
from parsl.config import Config
import tqdm
import ast

import pprint
class PrettyPrint(pprint.PrettyPrinter):
    def _format(self, object, *args, **kwargs):
        if isinstance(object, str):
            if len(object) > 20:
                object =  object[:20] + '...' + object[-10:]
        if isinstance(object, dict):
            new_dict = {}
            for key, val in object.items():
                if len(key) > 40:
                    nkey = key[:34] + "..."
                else:
                    nkey = key
                if len(val) > 4:
                    new_dict[nkey] = val[:1] + ["..."+str(len(val)-2)+" others..."] + val[-1:]
                else:
                    new_dict[nkey] = val
            object = new_dict
        return pprint.PrettyPrinter._format(self, object, *args, **kwargs)


from itertools import chain
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample')
    parser.add_argument('-i', '--input', required=True, help='Input file JSON.')
    parser.add_argument('-o', '--output', default=r'hashes.json', help='')
    parser.add_argument('-j', '--ncpu', type=int, default=10,
                        help="Number of CPUs to use with RDataFrame")
    parser.add_argument('--run', action='store_true', help='Bool')
    parser.add_argument('--parsl', action='store_true', help='Scale out via parsl/slurm')
    args = parser.parse_args()

    start = time.time()

    # @python_app
    # def get_branches(fname):
    #     rf = ROOT.TFile.Open(fname)
    #     to_save = [n.GetName() for n in rf.Get('Events').GetListOfBranches()]
    #     # Get a hash from branches to identify unique combinations of available/missing branches
    #     return hashlib.md5(bytes("".join(sorted(to_save)), 'utf-8')).hexdigest()

    @bash_app
    def batch_branches(filelist, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
        return f'python -b get_branches.py {" ".join(filelist)}'

    config = Config(executors=[ThreadPoolExecutor(max_threads=args.ncpu)])
    parsl.load(config)

    with open(args.input) as f:
        sample_dict = json.load(f)

    y = list(chain(*sample_dict.values()))

    futures = []
    file_chunks = []
    job_size = 100
    for i, chunk in enumerate(chunks(y, job_size)):
        futures.append(batch_branches(chunk))
        file_chunks.append(chunk)

    # Pbar until done
    with tqdm.tqdm(total=len(futures)) as pbar:
        unfinished = set(job for job in futures if not job.done())
        while len(unfinished) > 0:
            finished = set(job for job in futures if job.done())
            unfinished = set(job for job in futures if not job.done())
            pbar.update(len(finished) - pbar.n)
            time.sleep(0.2)

    hash_chunks = []
    for i, r in enumerate(futures):
        try:
            with open(r.stdout, 'r') as f:
                hash_chunks.append(ast.literal_eval(f.read()))
        except:
            print(f"Bad batch {i}, content:")
            print(chunks(y, job_size)[i])
            hash_chunks.append("X"*job_size)

    hash_dict = {}
    for names, hashes in zip(file_chunks, hash_chunks):
        for name, hash in zip(names, hashes):
            hash_dict[name] = hash

    print("Writing files to {}".format(args.output))
    with open(args.output, 'w') as fp:
        json.dump(hash_dict, fp, indent=4)