import sys, pprint
import zimDerivate

if len(sys.argv)!=3:
    print ("usage: ", sys.argv[0], "inputFile.zim outputfile.zim")
    sys.exit(1) 
    
inZim=open(sys.argv[1], "rb")
outZim=open(sys.argv[2], "w+b")

print ('parsing original zim...')
parseInZim=zimDerivate.ParseZim(inZim)
if parseInZim.head['magicNumber']!=72173914:
    print ("The given input file is not a zim file.")

print ("Creating derivated zim...")
derivatedZim=zimDerivate.DerivatedZim(inZim, outZim, parseInZim)

derivatedZim.convertCompress=4

print ("Copying clusters...")
derivatedZim.processCopy()

print ("updating checksum of derivated zim...");
derivatedZim.updateChecksum()
print('done!')
