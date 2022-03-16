import os
import sys
import time
import tqdm
import json
import warnings
from itertools import chain
from collections import defaultdict, OrderedDict

import argparse
import pandas as pd

from pathlib import Path


import parsl
from parsl import bash_app, python_app
# Hadd instance
@bash_app
def hadd_files(filelist=[], out='hadded.root', dry=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    job_string = f'python -b haddnano.py {out} {" ".join(filelist)}'
    if dry:
        print(job_string)
        return 0
    else:
        return job_string

# @bash_app
# def skim_files(filelist=[], out='hadded.root', dry=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
#     job_string = f'python -b haddnanodrop.py {out} {" ".join(filelist)}'
#     if dry:
#         print(job_string)
#         return 0
#     else:
#         return job_string
@bash_app
def skim_files(filelist=[], out='hadded.root', dry=False, branches=None, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    if branches is None:
        job_string = f'python -b modhaddnano {out} {" ".join(filelist)}'
    else:
        job_string = f'python -b modhaddnano {out} {" ".join(filelist)}  --branches={branches}'
    if dry:
        print(job_string)
        return 0
    else:
        return job_string

@bash_app
def check_files(filelist=[], out='hadded.root', dry=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    job_string = f'python -b check_hadd.py {out} {" ".join(filelist)}'
    if dry:
        print(job_string)
        return 0
    else:
        return job_string

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample')
    parser.add_argument('-i', '--input', help='Input JSON', required=True)
    parser.add_argument('-d', '--dir', help='Output directory', required=True)
    parser.add_argument('-o', '--output', default=r'out_dummy.json', help='New JSON')
    parser.add_argument('-s', '--hash', default=None,
                        help='File hash dict/JSON. Secondary group-by value for files with variable branches')
    parser.add_argument('-b', '--branches', default=None,
                        help='json (list) with branches to keep (works with --skim)')
    parser.add_argument('-m', type=int, default=4000,
                        help="MB size of input files to merge")
    parser.add_argument('-j', '--ncpu', type=int, default=10,
                        help="Number of CPUs to use with RDataFrame")
    parser.add_argument('--run', action='store_true', help='Bool')
    parser.add_argument('--skim', action='store_true', help='Bool')
    parser.add_argument('--test', action='store_true', help='Bool')
    parser.add_argument('--check', action='store_true', help='Bool')
    parser.add_argument('--dry', action='store_true', help='Bool')
    parser.add_argument('--parsl', action='store_true', help='Scale out via parsl/slurm')
    args = parser.parse_args()

    start = time.time()

    with open(args.input) as f:
        sample_dict = json.load(f)

    if args.test:
        dsets = list(chain(*[[dset] for dset, vals in sample_dict.items()]))
        fnames = list(chain(*[vals[:1] for _, vals in sample_dict.items()]))
    else:
        dsets = list(chain(*[[dset] * len(vals) for dset, vals in sample_dict.items()]))
        fnames = list(chain(*[vals for _, vals in sample_dict.items()]))
    # Check if exist
    exist = [os.path.exists(fn) for fn in fnames]
    fnames = [a for a, b in zip(fnames, exist) if b]
    dsets = [a for a, b in zip(dsets, exist) if b]   

    sizes = [os.stat(fn).st_size for fn in fnames]
    if args.hash is not None:
        with open(args.hash) as f:
            hashes_dict = json.load(f)
            if len(hashes_dict.keys()) != len(fnames):
                warnings.warn(f"Number of hash keys {len(hashes_dict.keys())} not the same as number of inputs {len(fnames)}")
        hashes = [hashes_dict[f] for f in fnames]
    else:
        answer = input("No hash dict provided. Merging will crash if files don't have\n"
                       "the same branches. Continue Y/N?")
        if answer.upper() in ["Y", "YES", ""]:
            pass
        else:
            sys.exit()
        hashes = ["" for _ in fnames]

    df = pd.DataFrame({
        'dataset': dsets,
        'file': fnames,
        'hash': hashes,
        'size': sizes,
    })

    print(df)

    target_size = int(args.m * 1024 * 1024)
    new_json = defaultdict(list)
    jobs = OrderedDict()
    for (dataset, form_hash), items in df.groupby(["dataset", "hash"]):
        for i, (_, job) in enumerate(items.groupby(items["size"].cumsum() // target_size)):
            batchlist = list(job.file)
            outpath = os.path.join(os.path.realpath(args.dir), dataset.strip('/'))
            Path(outpath).mkdir(parents=True, exist_ok=True)
            outpath = os.path.join(outpath, batchlist[0].split("/")[-1].lstrip("/")) # Per sample directory
            #outpath = os.path.join(outpath, fnames[0].split("/")[-1].lstrip("/")) # Per sample directory
            out = outpath.replace(".root", "") + f"_{form_hash}_{i}.root"

            new_json[dataset].append(out)
            if os.path.isfile(out) and not args.check:
                continue
            jobs[out] = batchlist

    print("Prepared {} merge jobs".format(len(jobs)))
    print("   with {} files in total.".format(len(list(chain(*[vals for _, vals in jobs.items()])))))
    print("Writing output paths to {}".format(args.output))
    with open(args.output, 'w') as fp:
        json.dump(new_json, fp, indent=4)
    joblog = args.output.split(".json")[0] + "_log.json"
    print("Writing job info to {}".format(joblog))
    with open(joblog, 'w') as fp:
        json.dump(jobs, fp, indent=4)

    # print("Validating jobs:")

    # def sfilter(s):
    #     if "13TeV" in s or "JetHT" in s or "Single" in s:
    #         return True
    #     else:
    #         return False

    # for i, key in enumerate(tqdm.tqdm(jobs.keys())):
    #     sample = [k for k in key.split("/") if sfilter(k)][0]
    #     uniques = list(set([[k for k in f.split("/") if sfilter(k)][0] for f in jobs[key]]))
    #     if len(uniques) == 1 and uniques[0] == sample:
    #         pass
    #     else:
    #         print("Samples in this job don't match -", key)
    #         print(uniques)

    # Execute
    if args.run:
        print("Processing:")
        if args.parsl:
            import parsl
            from parsl.app.app import python_app, bash_app
            from parsl.configs.local_threads import config
            from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
            from parsl.channels import LocalChannel
            from parsl.config import Config
            from parsl.executors import HighThroughputExecutor
            from parsl.launchers import SrunLauncher

            from parsl.addresses import address_by_hostname

            x509_proxy = 'x509up_u%s'%(os.getuid())
            wrk_init = '''
            ulimit -n 4000
            ulimit -u 32768
            '''
            slurm_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_slurm",
                        address=address_by_hostname(),
                        prefetch_capacity=0,
                        max_workers=20,
                        provider=SlurmProvider(
                            channel=LocalChannel(script_dir='parsl_slurm'),
                            launcher=SrunLauncher(),
                            max_blocks=args.ncpu,
                            init_blocks=args.ncpu,
                            partition='all',
                            worker_init=wrk_init,
                            walltime='08:00:00'
                        ),
                    )
                ],
                retries=0,
            )
            dfk = parsl.load(slurm_htex)
        else:
            from parsl.config import Config
            from parsl.executors.threads import ThreadPoolExecutor
            config = Config(executors=[ThreadPoolExecutor(max_threads=args.ncpu)])
            parsl.load(config)

        futures = []
        for i, (out, files) in enumerate(jobs.items()):
            # if "TTToHadronic_TuneCP5_13TeV-powheg-pythia8/nano_mc2018_12_a677915bd61e6c9ff968b87c36658d9d_268.root" not in out:
            #     continue
            # else:
            #     print("BOING")
            #     print(out)
            #     print(" ".join(files))
            if args.check:
                x = check_files(filelist=files, out=out, dry=args.dry)
            elif args.skim:
                x = skim_files(filelist=files, out=out, dry=args.dry, branches=args.branches)
            else:
                x = hadd_files(filelist=files, out=out, dry=args.dry)
            futures.append(x)

        # Pbar until done
        with tqdm.tqdm(total=len(futures)) as pbar:
            unfinished = set(job for job in futures if not job.done())
            while len(unfinished) > 0:
                finished = set(job for job in futures if job.done())
                unfinished = set(job for job in futures if not job.done())
                pbar.update(len(finished) - pbar.n)
                time.sleep(0.2)

        if args.check:
            total_in, total_out = 0, 0
            bad_outs = []
            for i, (out, files) in enumerate(jobs.items()):
                try:
                    with open(futures[i].stdout, 'r') as f:
                        counto, counti = f.read().split()
                        counto = int(counto)
                        counti = int(counti)
                        total_in += counti
                        total_out += counto
                        if counto != counti:
                            bad_outs.append(out)
                            print(f"Job {out} incomplete, {counto}/{counti} included.")
                except:
                    bad_outs.append(out)
            print(f"Total in = {total_in}, total out = {total_out}")

            if len(bad_outs) > 0:
                if input("Remove bad files? (y/n)") == "y":
                    print("Removing:")
                    for fi in bad_outs:
                        print(f"Removing: {fi}")
                        os.system(f'rm {fi}')

    print("Done")
    end = time.time()
    print("TIME:", time.strftime("%H:%M:%S", time.gmtime(end - start)))
