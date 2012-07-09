from __future__ import division
"""
Example for lheanalyzer

Looks for leading charged lepton and corresponding neutrino and plots
histograms for transverse mass and  invariant mass between them, 
using correct event weights and cross section.

This example requires pyROOT installed.
"""

import lheanalyzer
import ROOT
#Create lhe analysis from lhe file
analysis = lheanalyzer.LHEAnalysis("testfile.lhe")

# prepare histograms
hTransverseMass = ROOT.TH1F("hTransverseMass","",400,0,8000)
hInvariantMass = ROOT.TH1F("hInvariantMass","",400,0,8000)

#prepare dicts for double check of process ratios
weight, events = dict(), dict()
for process in analysis.processes:
    weight[process.id], events[process.id]=0, 0

# main analysis loop
for event in analysis:
    #Make a list of all charged leptons
    charged = filter(lambda particle: particle.pdgId in [-15,-13,-11,11,13,15], event.particles)
    #Sort list by pt
    charged = sorted(charged, key=lambda particle: particle.pt)
    
    #Make a list of all neutrinos
    neutrino = filter(lambda particle: particle.pdgId in [-16,-14,-12,12,14,16], event.particles)
    #Sort list by pt
    neutrino = sorted(neutrino, key=lambda particle: particle.pt)
    
    #calculate variables
    mass = lheanalyzer.invariantMass(charged[0],neutrino[0])
    transverseMass = lheanalyzer.transverseMass(charged[0],neutrino[0])
    
    #add to histogram and use weights
    hInvariantMass.Fill(mass,event.weight)
    hTransverseMass.Fill(transverseMass,event.weight)

    #save event weights for double check of process ratios
    weight[event.processId] += event.weight
    events[event.processId] += 1
    
#Do the cross check
crossSection = analysis.totalCrossSection
print "Ratio from weights and from xsecs should be about the same"
for p in analysis.processes:
    print "Process", p.id, "Ratio from weights", weight[p.id]/sum(weight.values()), "Ratio from xsec", p.crossSection/crossSection, "Ratio from events", events[p.id]/sum(events.values())

#rescale histograms to cross section
hInvariantMass.Scale(crossSection/hInvariantMass.Integral())
hTransverseMass.Scale(crossSection/hTransverseMass.Integral())
#draw histograms
hTransverseMass.SetLineColor(ROOT.kRed)
hInvariantMass.Draw("histo")
hTransverseMass.Draw("histo same")

#wait for return key so that the plots are shown
raw_input("Press return to exit")
    
