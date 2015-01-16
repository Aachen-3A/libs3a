import aix3adb
import pprint
def main():
    pp = pprint.PrettyPrinter(indent=4)
    # Ein Datenbank-Objekt erstellen
    dblink=aix3adb.aix3adbAuth(username="olschew")
    altessample = dblink.getMCSample(1)
    print "Get sample"
    pp.pprint(altessample.__dict__)
    
    ##Ein Sample erstellen
    print "Write sample"
    sample = aix3adb.MCSample()
    sample.name = "test"
    neuessample = dblink.registerMCSample(sample)

    pp.pprint(neuessample)
if __name__=="__main__":
    main()
