import coffea
from coffea import hist
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep
import uproot
import awkward as ak

#plt.style.use(hep.style.CMS)
xrd = 'root://cms-xrd-global.cern.ch//'
xr2 = "root://xrootd-cms.infn.it//"

f16 = [
    "/beegfs/desy/group/af-cms/ddc/v2nano16/store/user/anovak/PFNano/106X_v2_16/DYJetsToLL_M-50_HT-1200to2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16PFNanoAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1PFNanoNoInpt/201228_174758/0000/nano_mc2016_10.root",
    "/beegfs/desy/group/af-cms/ddc/v2nano16/store/user/anovak/PFNano/106X_v2_16rsb2/JetHT/Run2016E-17Jul2018-v1_PFNanoAODv2/210114_201155/0000/nano_data2016_153.root",
]

f17 = [
    "/beegfs/desy/group/af-cms/ddc/v2nano17/store/user/anovak/PFNano/106X_v2_17/DYJetsToLL_M-50_HT-1200to2500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17PFNanoAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1PFNanoV2/210101_181011/0000/nano_mc2017_3.root",
    "/beegfs/desy/group/af-cms/ddc/v2nano17/store/user/anovak/PFNano/106X_v2_17rsb2/JetHT/Run2017D-31Mar2018-v1_PFNanoAODv2/210114_202223/0000/nano_data2017_6.root",
    "here.root"
]

f18 = [
    "/beegfs/desy/group/af-cms/ddc/v2nano18/store/user/anovak/PFNano/106X_v2_18/DYJetsToLL_M-50_HT-1200to2500_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18PFNanoAOD-102X_upgrade2018_realistic_v15-v2PFNanov2/210104_014645/0000/nano_mc2018_6.root",
    "/beegfs/desy/group/af-cms/ddc/v2nano18/store/user/anovak/PFNano/106X_v2_18rsb/JetHT/Run2018C-17Sep2018-v1_PFNanoAODv2/210111_145409/0000/nano_data2018abc_33.root",
]

from coffea.nanoevents import NanoEventsFactory
from boostedhiggs.hbbprocessor import HbbProcessor

blist = []

# (self, year='2017', jet_arbitration='pt', v2=False, v3=False, v4=False,
#     nnlops_rew=False, skipJER=False, tightMatch=False, newTrigger=False, looseTau=False

for year, files in zip(['2016', '2017', '2018'], [f16, f17, f18]):
    fnames = ['GluGluHToCC_M125_13TeV_powheg_pythia8', 'JetHT']
    for name, file in zip(fnames, files):
        pconfs = []
        pconfs.append(HbbProcessor(v2=True, newTrigger=False, tightMatch=True, looseTau=True, year=year))
        pconfs.append(HbbProcessor(v3=True, newTrigger=True, year=year))

        factory = NanoEventsFactory.from_root(
            file,
            entry_stop=2000,
            metadata={"dataset": 'GluGluHToCC_M125_13TeV_powheg_pythia8'},
            access_log=blist,
        )
        events = factory.events()
        for p in pconfs:
            outn = p.process(events)
        print(len(set(blist)))

klist = sorted(list(set(blist)))
for key in klist:
    print(f"'{key}',")
