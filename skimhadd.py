#!/usr/bin/env python

import ROOT
ROOT.gROOT.SetBatch()
ROOT.gErrorIgnoreLevel = ROOT.kError
import os
import sys
#import time
import argparse
from pathlib import Path

def doFiles(filelist, out):
    fChain = ROOT.TChain("Events")
    for f in filelist:
        fChain.Add(f)
    isData = 'nGenPart' not in [branch.GetName()
                                for branch in fChain.GetListOfBranches()]

    d = ROOTDataFrame(fChain)
    # Pre-selection for 2-Lep SR
    sel = "Sum(FatJet_pt>350)>0"
    # sel = "2+2 == 4"
    # print("Applying selection:")
    # print("  "+sel)
    filt_0 = d.Filter(sel)
    # More filters can be added if needed
    dOut = filt_0
    # This is list of variables for Varial and BDT
    varsToSave = ['nElectron', 'nFatJet', 'nJet', 'nMuon', 'nSubJet', 'nTau', 'event', 'Electron_cutBased', 'Electron_eta', 'Electron_pt', 'FatJet_n2b1', 'FatJet_btagDDBvLV2', 'FatJet_btagDDCvBV2', 'FatJet_btagDDCvLV2', 'FatJet_eta', 'FatJet_jetId', 'FatJet_phi', 'FatJet_pt', 'HLT_AK8PFJet500',
                  'HLT_Mu50', 'HLT_PFHT1050', 'HLT_PFJet500', 'Jet_btagDeepB', 'Jet_eta', 'Jet_jetId', 'Jet_phi', 'Jet_pt', 'Muon_eta', 'Muon_looseId', 'Muon_pfRelIso04_all', 'Muon_phi', 'Muon_pt', 'MET_pt', 'SubJet_eta', 'SubJet_mass', 'SubJet_phi', 'SubJet_pt', 'SubJet_rawFactor', 'Tau_idDecayMode', 'Tau_pt']

    if isData:
        varsToSave += ['HLT_AK8PFJet500', 'HLT_Mu50',
                       'HLT_PFHT1050', 'HLT_PFJet500']
    else:
        varsToSave += ['nGenPart',
                       'GenPart_eta',
                       'GenPart_genPartIdxMother',
                       'GenPart_pdgId',
                       'GenPart_phi',
                       'GenPart_pt',
                       'GenPart_statusFlags']

    outVars = ROOT.std.vector('string')()
    for v in varsToSave:
        outVars.push_back(v)

    dOut.Snapshot("Events", out, outVars)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample', usage="./SkimTrees.py sample_name")
    parser.add_argument('out', type=str)        
    parser.add_argument('filenames', metavar='FILES', type=str, nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()

    TH1DModel = ROOT.RDF.TH1DModel
    ROOTDataFrame = ROOT.RDataFrame
    usingRDF = True
    #ROOT.ROOT.EnableImplicitMT(1)

    #start = time.time()
    doFiles(args.filenames, args.out)
    #end = time.time()
    #print("TIME:", time.strftime("%H:%M:%S", time.gmtime(end-start)))