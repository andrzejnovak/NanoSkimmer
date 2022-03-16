#!/usr/bin/env python3
import numpy
import sys
import json
import gzip
import subprocess
import urllib.parse
import ROOT
import time
ROOT.gROOT.SetBatch()

if len(sys.argv) < 3:
    print("Syntax: haddnano.py out.root input1.root input2.root ...")
ofname = sys.argv[1]
files = sys.argv[2:]

goFast = True
optimize_debug = False
delete_input = False
# https://root.cern.ch/doc/master/structROOT_1_1RCompressionSetting.html
#  ZLIB is recommended to be used with compression level 1 [101]
#  LZMA is recommended to be used with compression level 7-8 (higher is better, since in the case of LZMA we don't care about compression/decompression speed) [207 - 208]
#  LZ4 is recommended to be used with compression level 4 [404]
#  ZSTD is recommended to be used with compression level 5 [505]
newcompression = 404
iofeatures = ROOT.TIOFeatures()
iofeatures.Set(ROOT.Experimental.EIOFeatures.kGenerateOffsetMap)
tic = time.time()


def zeroFill(tree, brName, brObj, allowNonBool=False):
    # typename: (numpy type code, root type code)
    branch_type_dict = {
        "Bool_t": ("?", "O"),
        "Float_t": ("f4", "F"),
        "UInt_t": ("u4", "i"),
        "Long64_t": ("i8", "L"),
        "Double_t": ("f8", "D"),
    }
    brType = brObj.GetLeaf(brName).GetTypeName()
    if (not allowNonBool) and (brType != "Bool_t"):
        print(
            (
                "Did not expect to back fill non-boolean branches",
                tree,
                brName,
                brObj.GetLeaf(br).GetTypeName(),
            )
        )
    else:
        if brType not in branch_type_dict:
            raise RuntimeError("Impossible to backfill branch of type %s" % brType)
        buff = numpy.zeros(1, dtype=numpy.dtype(branch_type_dict[brType][0]))
        b = tree.Branch(brName, buff, brName + "/" + branch_type_dict[brType][1])
        # be sure we do not trigger flushing
        b.SetBasketSize(tree.GetEntries() * 2)
        for x in range(0, tree.GetEntries()):
            b.Fill()
        b.ResetAddress()


fileHandles = []
totbytes = 0
for fn in files:
    print("Adding file " + str(fn))
    fileHandles.append(ROOT.TFile.Open(fn))
    totbytes += fileHandles[-1].GetSize()
    if (
        fileHandles[-1].GetCompressionSettings()
        != fileHandles[0].GetCompressionSettings()
    ):
        goFast = False
        print("Disabling fast merging as inputs have different compressions")

print("Input compression: " + str(fileHandles[0].GetCompressionSettings()))

of = ROOT.TFile(ofname, "recreate")
if goFast:
    of.SetCompressionSettings(fileHandles[0].GetCompressionSettings())
else:
    of.SetCompressionSettings(newcompression)
of.cd()

for e in fileHandles[0].GetListOfKeys():
    name = e.GetName()
    if name == "tag":
        continue
    print("Merging " + str(name))
    obj = e.ReadObj()
    inputs = ROOT.TList()
    isTree = obj.IsA().InheritsFrom(ROOT.TTree.Class())
    if isTree:
        if obj.GetName() == "Events":
            obj.OptimizeBaskets(100*1000000, 1.1, "d" if optimize_debug else "")
        ROOT.SetOwnership(obj, True)
        obj = obj.CloneTree(-1, "fast" if goFast else "")
        branchNames = set([x.GetName() for x in obj.GetListOfBranches()])
        obj.SetIOFeatures(iofeatures)
    for fh in fileHandles[1:]:
        otherObj = fh.GetListOfKeys().FindObject(name).ReadObj()
        inputs.Add(otherObj)
        if isTree and obj.GetName() == "Events":
            otherObj.SetAutoFlush(0)
            otherBranches = set([x.GetName() for x in otherObj.GetListOfBranches()])
            missingBranches = list(branchNames - otherBranches)
            additionalBranches = list(otherBranches - branchNames)
            if missingBranches or additionalBranches:
                print(
                    "missing: "
                    + str(missingBranches)
                    + "\n Additional:"
                    + str(additionalBranches)
                )
            for br in missingBranches:
                # fill "Other"
                zeroFill(otherObj, br, obj.GetListOfBranches().FindObject(br))
            for br in additionalBranches:
                # fill main
                branchNames.add(br)
                zeroFill(obj, br, otherObj.GetListOfBranches().FindObject(br))
            # merge immediately for trees
        if isTree and obj.GetName() == "Runs":
            otherObj.SetAutoFlush(0)
            otherBranches = set([x.GetName() for x in otherObj.GetListOfBranches()])
            missingBranches = list(branchNames - otherBranches)
            additionalBranches = list(otherBranches - branchNames)
            if missingBranches or additionalBranches:
                print(
                    "missing: "
                    + str(missingBranches)
                    + "\n Additional:"
                    + str(additionalBranches)
                )
            for br in missingBranches:
                # fill "Other"
                zeroFill(
                    otherObj,
                    br,
                    obj.GetListOfBranches().FindObject(br),
                    allowNonBool=True,
                )
            for br in additionalBranches:
                # fill main
                branchNames.add(br)
                zeroFill(
                    obj,
                    br,
                    otherObj.GetListOfBranches().FindObject(br),
                    allowNonBool=True,
                )
            # merge immediately for trees
        if isTree:
            obj.Merge(inputs, "fast" if goFast else "")
            inputs.Clear()

    if isTree:
        obj.Write()
    elif obj.IsA().InheritsFrom(ROOT.TH1.Class()):
        obj.Merge(inputs)
        inputs.Clear()
        obj.Write()
    elif obj.IsA().InheritsFrom(ROOT.TObjString.Class()):
        for st in inputs:
            if st.GetString() != obj.GetString():
                print("Strings are not matching")
        obj.Write()
    else:
        print("Cannot handle " + str(obj.IsA().GetName()))


print("Sum of input files: %d bytes" % totbytes) 
print("Output file: %d bytes" % of.GetSize()) 
print("Fraction of original: %f" % (of.GetSize() * 1. / totbytes))
of.Close()

print("TIME:", time.strftime("%H:%M:%S", time.gmtime(time.time() - tic)))