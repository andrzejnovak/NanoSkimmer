#!/usr/bin/env python3
import sys
import ROOT
ROOT.gROOT.SetBatch()

if len(sys.argv) < 3:
    print("Syntax: check_hadd.py out.root input1.root input2.root ...")
ofname = sys.argv[1]
files = sys.argv[2:]

try:
    orf = ROOT.TFile.Open(ofname)
    merged = orf.Get('Events').GetEntries()

    fChain = ROOT.TChain("Events")
    for f in files:
        try:
            fChain.Add(f)
        except:
            pass
    incoming = fChain.GetEntries()

    print(merged, incoming)
except:
    print(0, 0)


