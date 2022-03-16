import ROOT
import argparse
import hashlib

def branches(fname):
    rf = ROOT.TFile.Open(fname)
    to_save = [n.GetName() for n in rf.Get('Events').GetListOfBranches()]
    # Get a hash from branches to identify unique combinations of available/missing branches
    return hashlib.md5(bytes("".join(sorted(to_save)), 'utf-8')).hexdigest() 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Skim and add trees for a sample', usage="./SkimTrees.py sample_name")
    parser.add_argument('filenames', metavar='FILES', type=str, nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()

    print([branches(fname) for fname in args.filenames])