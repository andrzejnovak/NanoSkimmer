#!/usr/bin/env python
import argparse
import os
import hashlib
import ROOT
ROOT.gROOT.SetBatch()
ROOT.gErrorIgnoreLevel = ROOT.kError

# This is list of variables (nVar have to be first)
varsToSave = [
'Electron_cutBased,
'Electron_eta,
'Electron_pt,
'FatJet_area,
'FatJet_btagDDBvLV2,
'FatJet_btagDDCvBV2,
'FatJet_btagDDCvLV2,
'FatJet_eta,
'FatJet_genJetAK8Idx,
'FatJet_jetId,
'FatJet_mass,
'FatJet_n2b1,
'FatJet_phi,
'FatJet_pt,
'FatJet_rawFactor,
'FatJet_subJetIdx1,
'FatJet_subJetIdx2,
'GenJetAK8_pt,
'GenJet_pt,
'GenPart_eta,
'GenPart_genPartIdxMother,
'GenPart_pdgId,
'GenPart_phi,
'GenPart_pt,
'GenPart_statusFlags,
'HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV_p20,
'HLT_AK8PFHT700_TrimR0p1PT0p03Mass50,
'HLT_AK8PFHT800_TrimMass50,
'HLT_AK8PFJet330_TrimMass30_PFAK8BoostedDoubleB_np4,
'HLT_AK8PFJet360_TrimMass30,
'HLT_AK8PFJet400_TrimMass30,
'HLT_AK8PFJet420_TrimMass30,
'HLT_AK8PFJet500,
'HLT_Mu50,
'HLT_PFHT1050,
'HLT_PFHT650_WideJetMJJ900DEtaJJ1p5,
'HLT_PFHT650_WideJetMJJ950DEtaJJ1p5,
'HLT_PFHT800,
'HLT_PFHT900,
'HLT_PFJet450,
'HLT_PFJet500,
'Jet_area,
'Jet_btagDeepB,
'Jet_eta,
'Jet_genJetIdx,
'Jet_hadronFlavour,
'Jet_jetId,
'Jet_mass,
'Jet_phi,
'Jet_pt,
'Jet_rawFactor,
'LHEScaleWeight,
'MET_MetUnclustEnUpDeltaX,
'MET_MetUnclustEnUpDeltaY,
'MET_phi,
'MET_pt,
'Muon_eta,
'Muon_looseId,
'Muon_pfRelIso04_all,
'Muon_phi,
'Muon_pt,
'Muon_tightId,
'Pileup_nPU,
'SubJet_eta,
'SubJet_mass,
'SubJet_phi,
'SubJet_pt,
'SubJet_rawFactor,
'Tau_idDecayMode,
'Tau_pt,
'fixedGridRhoFastjetAll,
'genWeight,
'luminosityBlock,
'nElectron,
'nFatJet,
'nGenJet,
'nGenJetAK8,
'nGenPart,
'nJet,
'nMuon,
'nSubJet,
'nTau,
'run,
]


def doFiles(filelist, out):
    # Actual skimming
    if os.path.isfile(out):
        return 0
    else:
        fChain = ROOT.TChain("Events")
        for f in filelist:
            fChain.Add(f)
        available = sorted([branch.GetName() for branch in fChain.GetListOfBranches()])
        # Put counting branches first
        available = [a for a in available if a.startswith("n")] + [a for a in available if not a.startswith("n")]

        events_df = ROOT.RDataFrame(fChain)
        # Pre-selection
        # sel = "Sum(FatJet_pt>350)>0"
        sel = "2+2 == 4"
        events_fil0 = events_df.Filter(sel)
        # More filters can be added if needed
        events_out = events_fil0

        outVars = ROOT.std.vector('string')()
        for v in available:
            if v in varsToSave:
                outVars.push_back(v)

        events_out.Snapshot("Events", out, outVars)

        fChain.Reset()
        del fChain
        del events_df
        del events_out
        del outVars
    # print(outs)  # prnt output names, to be collected by parsl
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Skim and add trees for a sample',
                                     usage="./SkimTrees.py sample_name")
    parser.add_argument('out', type=str)
    parser.add_argument('filenames',
                        metavar='FILES',
                        type=str,
                        nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()

    #start = time.time()
    doFiles(args.filenames, args.out)
    #end = time.time()
    #print("TIME:", time.strftime("%H:%M:%S", time.gmtime(end-start)))
