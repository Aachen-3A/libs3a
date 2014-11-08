import aix3adb
import pprint
def main():
    # Ein Datenbank-Objekt erstellen
    pp = pprint.PrettyPrinter(indent=4)

    dblink=aix3adb.aix3adb()
    # Ein Sample holen
    result = dblink.searchMCSamples({"original_name":"/WJetsToLNu_M-1000_TuneZ2star_8TeV-madgraph/Summer12-START53_V19_FSIM/AODSIM"})
    #result = dblink.getMCSample(11)
    pp.pprint(result)
    
    
    ##Ein Sample erstellen
    #files=["/neuertag","1/2/3.root"]
    #tags = dict()
    #tags['MUSiC_test']="blub2";
    #tags['MUSiC_test2']="blub3";
    #sample = dict()
    #sample['original_name'] = "/neuertag"
    #sample['skimmer_name'] = "CMS"
    #sample['skimmer_version'] = "1.23.4"
    #sample['skimmer_cmssw'] = "CMSSW_1_2_3"
    #sample['energy'] = 8
    #sample['crosssection'] = 1.234
    #sample['crosssection_reference'] = ""
    #sample['kfactor'] = 1
    #sample['kfactor_reference'] = ""
    #sample['prep_key'] = 0
    #sample['parentsample'] = 0
    #sample = dict()
    #sample['description'] = "bla"
    ##sample['files'] = files
    ##sample['tags'] = tags

    ##Zum Schreiben muss man sich zunaechst autorisieren (lesen geht auch ohne)
    #dblink.authorize(username="olschew")
    #neuessample = dblink.test()
    #print sample
    #print "-----"
    #neuessample = dblink.registerMCSample(sample)
    ##neuessample = dblink.insertOrReplaceMCTags(2,tags)
    #print neuessample
    #dblink.destroyauth()
if __name__=="__main__":
    main()
