#!/usr/bin/env python

import ROOT
import os
import sys
from subprocess import check_output
import json
import time
import argparse
from pathlib import Path


def createDir(myDir):
    if not os.path.exists(myDir):
        os.makedirs(myDir)


def doFile(filelist, out, singlefile=True):
    fChain = ROOT.TChain("Events")

    for f in filelist:
        fChain.Add(f)

    d = ROOTDataFrame(fChain)

    # Pre-selection for 2-Lep SR
    sel = "Sum(FatJet_pt>100)"
    print("Applying selection:")
    print("  "+sel)
    filt_0 = d.Filter(sel)
    # More filters can be added if needed
    dOut = filt_0
    # This is list of variables for Varial and BDT
    varsToSave = ['Electron_cutBased',
                  'Electron_eta',
                  'Electron_pt',
                  'FatJet_btagDDBvLV2',
                  'FatJet_btagDDCvBV2',
                  'FatJet_btagDDCvLV2',
                  'FatJet_eta',
                  'FatJet_jetId',
                  'FatJet_n2b1',
                  'FatJet_phi',
                  'FatJet_pt',
                  'GenPart_eta',
                  'GenPart_genPartIdxMother',
                  'GenPart_genPartIdxMother',
                  'GenPart_pdgId',
                  'GenPart_phi',
                  'GenPart_pt',
                  'GenPart_statusFlags',
                  'Jet_btagDeepB',
                  'Jet_eta',
                  'Jet_hadronFlavour',
                  'Jet_jetId',
                  'Jet_phi',
                  'Jet_pt',
                  'MET_pt',
                  'Muon_eta',
                  'Muon_looseId',
                  'Muon_pfRelIso04_all',
                  'Muon_phi',
                  'Muon_pt',
                  'Pileup_nPU',
                  'SubJet_eta',
                  'SubJet_mass',
                  'SubJet_phi',
                  'SubJet_pt',
                  'SubJet_rawFactor',
                  'Tau_idDecayMode',
                  'Tau_pt',
                  'event',
                  'genWeight',
                  'nElectron',
                  'nFatJet',
                  'nGenPart',   
                  'nGenPart',
                  'nGenPart',
                  'nGenPart',
                  'nGenPart',
                  'nJet',
                  'nMuon',
                  'nSubJet',
                  'nTau']

    outVars = ROOT.std.vector('string')()
    for v in varsToSave:
        outVars.push_back(v)

    if singlefile:
        dOut.Snapshot("Events", out, outVars)
    else:
        raise NotImplemented


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample', usage="./SkimTrees.py sample_name")
    parser.add_argument('-i', '--input', default=r'dummy.json', help='')
    parser.add_argument('-d', '--dir', help='Output directory', required=True)
    parser.add_argument('-o', '--output', default=r'dummy_skim.json', help='')
    parser.add_argument('--limit', default=1, help='')

    parser.add_argument('-j', '--ncpu', type=int, default=1,
                        help="Number of CPUs to use with RDataFrame")
    parser.add_argument('--run', help='Bool', action='store_true')
    args = parser.parse_args()

    rootver = ROOT.gROOT.GetVersion()
    if '/' in rootver:
        rootver = rootver.split('/')[0]
    if float(rootver) >= 6.14:
        TH1DModel = ROOT.RDF.TH1DModel
        ROOTDataFrame = ROOT.RDataFrame
        usingRDF = True
        ROOT.ROOT.EnableImplicitMT(args.ncpu)

    else:
        TH1DModel = ROOT.Experimental.TDF.TH1DModel
        ROOTDataFrame = ROOT.Experimental.TDataFrame

    start = time.time()

    with open(args.input) as f:
        sample_dict = json.load(f)

    print("Storage dir:")
    print("   ", os.path.abspath(args.dir))

    out_dict = {}
    for key in sample_dict.keys():
        new_list = []
        print(key)
        for i, fname in enumerate(sample_dict[key]):
            if i >= args.limit:
                continue
            outpath = os.path.join(os.path.abspath(args.dir), key.strip('/'))
            Path(outpath).mkdir(parents=True, exist_ok=True)
            out = os.path.join(outpath, fname.split("/")[-1].strip("/"))
            new_list.append(out)
            if args.run:
                doFile([fname], out)

        out_dict[key] = new_list

    print("Writing files to {}".format(args.output))
    with open(args.output, 'w') as fp:
        json.dump(out_dict, fp, indent=4)

    end = time.time()


#     if opt.sample == 'all':
#         #print 'Run over all samples'
#         #print 'Will submit on condor'

#         createDir('outCondor')

#         with open("skimmer_runscript.sh",'w') as runf:

#             cond_run = '''#!/bin/bash

# export ORIG_DIR=$PWD
# cd /cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw/CMSSW_10_6_0/src/
# source /cvmfs/cms.cern.ch/cmsset_default.sh
# eval `scramv1 runtime -sh`
# cd $ORIG_DIR
# ls
# echo "** Running parameters: "
# echo $1 $2 $3
# echo "Will start skimming sample $1"
# ./SkimTrees.py $1 -j $2 $3
# '''
#             if opt.lxplus:
#                 cond_run += """
# xrdcp -f sum_$1.root root://eoscms.cern.ch/%s
# rm sum_$1.root
# """ % out_path
#             runf.write(cond_run)

#         with open("skimmer_config.sub",'w') as subf:

#             if opt.lxplus:
#                 cond_conf = '''universe = vanilla
# Executable     =  skimmer_runscript.sh
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
# Notification     = never
# transfer_input_files = SkimTrees.py
# requirements = OpSysAndVer == "CentOS7"
# request_cpus = %NCPU%
# transfer_output_files=""
# Output = outCondor/job_$(Cluster)_$(Process).out
# Error  = outCondor/job_$(Cluster)_$(Process).err
# Log    = outCondor/job_$(Cluster)_$(Process).log
# +MaxRuntime = 21600
# Queue Arguments from (
# %ARGUMENTS%)
# '''
#             else:
#                 cond_conf = '''universe = vanilla
# Executable     =  skimmer_runscript.sh
# Should_Transfer_Files     = YES
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
# Notification     = never
# transfer_input_files = SkimTrees.py
# WhenToTransferOutput=On_Exit
# requirements = OpSysAndVer == "CentOS7"
# request_cpus = %NCPU%
# Output = outCondor/job_$(Cluster)_$(Process).out
# Error  = outCondor/job_$(Cluster)_$(Process).err
# Log    = outCondor/job_$(Cluster)_$(Process).log
# ##+RequestRuntime = 100000
# Queue Arguments from (
# %ARGUMENTS%)
# '''
#             args_for_cond = ''
#             for samp in os.listdir(in_path):
#                 if 'hadd' in samp: continue
#                 if 'skim' in samp: continue
#                 if len([i for i in os.listdir(out_path) if samp in i])>0: continue
#                 if opt.lxplus: args_for_cond += '%s %i --lxplus\n'% (samp, opt.ncpu)
#                 else: args_for_cond += '%s %i\n'% (samp, opt.ncpu)
#                 # #print samp
#             # #print args_for_cond
#             cond_conf = cond_conf.replace("%ARGUMENTS%", args_for_cond).replace("%NCPU%", str(opt.ncpu))
#             #print cond_conf
#             subf.write(cond_conf)

#     else:
#         doSample(opt.sample)
