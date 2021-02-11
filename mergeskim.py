#!/usr/bin/env python

from pathlib import Path
import argparse
import sys
import os
import ROOT
ROOT.gROOT.SetBatch()
ROOT.gErrorIgnoreLevel = ROOT.kError
#import time

# This is list of variables (nVar have to be first)
varsToSave = [
    'nElectron',
    'nFatJet',
    'nGenJet',
    'nGenJetAK8',
    'nGenPart',
    'nJet',
    'nMuon',
    'nSubJet',
    'nTau',
    'Electron_cutBased',
    'Electron_eta',
    'Electron_pt',
    'event',
    'FatJet_area',
    'FatJet_btagDDBvLV2',
    'FatJet_btagDDCvBV2',
    'FatJet_btagDDCvLV2',
    'FatJet_eta',
    'FatJet_genJetAK8Idx',
    'FatJet_jetId',
    'FatJet_mass',
    'FatJet_matched_gen',
    'FatJet_n2b1',
    'FatJet_particleNet_HbbvsQCD',
    'FatJet_particleNet_HccvsQCD',
    'FatJet_particleNetMD_Xbb',
    'FatJet_particleNetMD_Xcc',
    'FatJet_phi',
    'FatJet_pt',
    'FatJet_rawFactor',
    'FatJet_subJetIdx1',
    'FatJet_subJetIdx2',
    'fixedGridRhoFastjetAll',
    'GenJet_eta',
    'GenJet_mass',
    'GenJet_partonFlavour',
    'GenJet_phi',
    'GenJet_pt',
    'GenJetAK8_eta',
    'GenJetAK8_mass',
    'GenJetAK8_partonFlavour',
    'GenJetAK8_phi',
    'GenJetAK8_pt',
    'GenPart_eta',
    'GenPart_genPartIdxMother',
    'GenPart_pdgId',
    'GenPart_phi',
    'GenPart_pt',
    'GenPart_statusFlags',
    'genWeight',
    'HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV_p20',
    'HLT_AK8PFHT700_TrimR0p1PT0p03Mass50',
    'HLT_AK8PFHT800_TrimMass50',
    'HLT_AK8PFJet330_PFAK8BTagCSV_p17',
    'HLT_AK8PFJet330_TrimMass30_PFAK8BoostedDoubleB_np4',
    'HLT_AK8PFJet360_TrimMass30',
    'HLT_AK8PFJet400_TrimMass30',
    'HLT_AK8PFJet420_TrimMass30',
    'HLT_AK8PFJet500',
    'HLT_Mu50',
    'HLT_PFHT1050',
    'HLT_PFHT650_WideJetMJJ900DEtaJJ1p5',
    'HLT_PFHT800',
    'HLT_PFHT900',
    'HLT_PFJet450'
    'HLT_PFJet500',
    'HLT_TkMu50',
    'Jet_area',
    'Jet_btagDeepB',
    'Jet_eta',
    'Jet_genJetIdx',
    'Jet_hadronFlavour',
    'Jet_jetId',
    'Jet_mass',
    'Jet_matched_gen',
    'Jet_phi',
    'Jet_pt',
    'Jet_rawFactor',
    'luminosityBlock',
    'MET_MetUnclustEnUpDeltaX',
    'MET_MetUnclustEnUpDeltaY',
    'MET_phi',
    'MET_pt',
    'Muon_eta',
    'Muon_looseId',
    'Muon_pfRelIso04_all',
    'Muon_phi',
    'Muon_pt',
    'Muon_tightId',
    'Pileup_nPU',
    'run', 
    'SubJet_eta',
    'SubJet_mass',
    'SubJet_phi',
    'SubJet_pt',
    'SubJet_rawFactor',
    'Tau_idDecayMode',
    'Tau_pt',
]

def doFiles(filelist, out):
    # Group by because Run2016B doesn't have all the same branches
    blist_counts = {}
    blist_available = {}
    for f in filelist:
        rf = ROOT.TFile.Open(f)
        to_save = [n.GetName() for n in rf.Get('Events').GetListOfBranches() if n.GetName() in varsToSave]
        blist_counts[f] = len(to_save)
        blist_available[len(to_save)] = to_save
    grouped = {}
    for key, value in sorted(blist_counts.items()):
        grouped.setdefault(value, []).append(key)

    # Actual skimming
    outs = []
    for i, (branch_number, fileset) in enumerate(grouped.items()):
        out_name = out.replace(".root", "") + f"_{i}.root"
        outs.append(out_name)
        if os.path.isfile(out_name):
            continue
        else:
            fChain = ROOT.TChain("Events")   
            for f in fileset:
                fChain.Add(f)
            available = [branch.GetName() for branch in fChain.GetListOfBranches()]
            isData = 'nGenPart' not in available

            events_df = ROOT.RDataFrame(fChain)
            # Pre-selection
            # sel = "Sum(FatJet_pt>350)>0"
            sel = "2+2 == 4"
            events_fil0 = events_df.Filter(sel)
            # More filters can be added if needed
            events_out = events_fil0

            outVars = ROOT.std.vector('string')()
            for v in blist_available[branch_number]:
                #if v in available:
                outVars.push_back(v)

            events_out.Snapshot("Events", out_name, outVars)

            fChain.Reset()
            del fChain
            del events_df
            del events_out
            del outVars
    print(outs) # print output names, to be collected by parsl
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample', usage="./SkimTrees.py sample_name")
    parser.add_argument('out', type=str)
    parser.add_argument('filenames', metavar='FILES', type=str, nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()

    # TH1DModel = ROOT.RDF.TH1DModel
    # ROOTDataFrame = ROOT.RDataFrame
    # usingRDF = True
    # ROOT.ROOT.EnableImplicitMT(1)

    #start = time.time()
    doFiles(args.filenames, args.out)
    #end = time.time()
    #print("TIME:", time.strftime("%H:%M:%S", time.gmtime(end-start)))
