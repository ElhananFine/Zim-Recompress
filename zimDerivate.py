import struct, os, hashlib, subprocess
import lzma , zstd 
import pprint

def getStrings(handle, nbStrings):
    result=[]
    data = b''
    origPos=handle.tell()
    
    while (len(result)<nbStrings):
        data+=handle.read(1024)
        result=data.split(b'\x00')[:nbStrings] #the [:xxx] makes a "slice"
        if(len(data)>50000):
            print (data)
            print ('too long ('+str(en(data))+') !!!')
    
    readenLen=0
    for i in range(len(result)):
        readenLen+=len(result[i])+1 # +1 for the separator*
        result[i]=result[i].decode()
    handle.seek(origPos+readenLen)
    
    return result


class ParseZim:
    MIME_REDIRECT=0xffff
    MIME_LINKTARGET=0xfffe
    MIME_DELETED=0xfffd


    def __init__(self, zim):
        self.zim=zim
        self.fileSize=os.path.getsize(self.zim.name)
        
        self.parseHead()
        if self.head['magicNumber']!=72173914:
            return False

        self.parseMimes()
        self.parseUrlPtr()
        self.parseClusters()

    def parseHead(self):
        self.zim.seek(0)
        self.head={}
        self.head['magicNumber']=struct.unpack('I', self.zim.read(4))[0]
        self.head['majorVersion']=struct.unpack('H', self.zim.read(2))[0]
        self.head['minorVersion']=struct.unpack('H', self.zim.read(2))[0]
        self.head['uuidA']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['uuidB']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['articleCount']=struct.unpack('I', self.zim.read(4))[0]
        self.head['clusterCount']=struct.unpack('I', self.zim.read(4))[0]
        self.head['urlPtrPos']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['titlePtrPos']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['clusterPtrPos']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['mimeListPos']=struct.unpack('Q', self.zim.read(8))[0]
        self.head['mainPage']=struct.unpack('I', self.zim.read(4))[0]
        self.head['layoutPage']=struct.unpack('I', self.zim.read(4))[0]
        self.head['checksumPos']=struct.unpack('Q', self.zim.read(8))[0]
    
    
    def parseMimes(self):
        self.mimes = [];
        actEntree = ''
        
        self.zim.seek(self.head['mimeListPos'])
        while True:
            octet=self.zim.read(1);
            if octet[0] != 0:
                actEntree+=octet.decode()
            else:
                if len(actEntree) != 0:
                    self.mimes.append(actEntree)
                    actEntree=''
                else:
                    break

    def parseUrlPtr (self):
        self.zim.seek(self.head['urlPtrPos'])
        self.urlPtrList=struct.unpack('Q'*self.head['articleCount'], self.zim.read(8*self.head['articleCount']))


    def parseEntriesCB(self, callback):
        self.zim.seek(self.urlPtrList[0]);
        pos=0
        
        while pos<self.head['articleCount']:
            callback(readEntry(self))
            pos+=1



    def findEntry(self, nameSpace, url):
        minpos=0
        maxpos=self.head['articleCount']-1
        
        while True:
          
          pos=minpos+(maxpos-minpos)//2 #We could make interpolation search to go faster... 
          
          entry=self.getEntryByUrlPtr(pos)
          
          if entry['nameSpace']==nameSpace and entry['url']==url:
            return entry
          
          
          if minpos == maxpos:
            return False
          
          if entry['nameSpace']+entry['url'] > nameSpace+url:
            maxpos=max(pos-1, minpos)
          else:
            minpos=min(pos+1, maxpos)
          
    def debugTitlePtr(self):
        for pos in range(self.head['articleCount']):
            entry=self.getEntryByTitlePtr(pos)
            print ('['+entry['nameSpace']+'] '+entry['url'])

            
    def getEntryByTitlePtr(self, pos):
        self.zim.seek(self.head['titlePtrPos']+pos*4)
        idEntry=struct.unpack('I', self.zim.read(4))[0]
        self.zim.seek(self.urlPtrList[idEntry])
        return self.readEntry()
      
    def getEntryByUrlPtr(self, pos):
        self.zim.seek(self.urlPtrList[pos])
        return self.readEntry()
        

    def readEntry(self):
            posInFile=self.zim.tell()
            idMime=struct.unpack('H', self.zim.read(2))[0]
            if idMime==self.MIME_REDIRECT:
                data=self.readEntryRedirect()
            elif  idMime==self.MIME_LINKTARGET or idMime==self.MIME_DELETED:
                data=self.readEntryTarget()
            else:
                data=self.readEntryArticle()
            
            data['mimeType']=idMime
            data['posInFile']=posInFile

            return data

    def readEntryTarget(self):
        data={}
        data['parameterLen']=struct.unpack('B', self.zim.read(1))[0]
        data['nameSpace']=self.zim.read(1).decode()
        data['revision']=struct.unpack('I', self.zim.read(4))[0]
        [data['url'], data['title']]=getStrings(self.zim, 2)
        if(data['parameterLen']>0):
            data['parameters']=self.zim.read(data['parameterLen'])
        
        return data

            
            
    #used by readEntry to parse redirect entry
    def readEntryRedirect(self):
        data={}
        data['parameterLen']=struct.unpack('B', self.zim.read(1))[0]
        data['nameSpace']=self.zim.read(1).decode()
        data['revision']=struct.unpack('I', self.zim.read(4))[0]
        data['targetDirEntry']=struct.unpack('I', self.zim.read(4))[0]
        [data['url'], data['title']]=getStrings(self.zim, 2)
        
        if(data['parameterLen']>0):
            data['parameters']=self.zim.read(data['parameterLen'])

        return data


    #used by readEntry to parse article entry
    def readEntryArticle (self):
        data={}
        data['parameterLen']=struct.unpack('B', self.zim.read(1))[0]
        data['nameSpace']=self.zim.read(1).decode()
        data['revision']=struct.unpack('I', self.zim.read(4))[0]
        data['cluster']=struct.unpack('I', self.zim.read(4))[0]
        data['blob']=struct.unpack('I', self.zim.read(4))[0]
        [data['url'], data['title']]=getStrings(self.zim, 2)
        
        if(data['parameterLen']>0):
            data['parameters']=self.zim.read(data['parameterLen'])

        return data
    
    def offsetAfterClusters(self):
      nextEltPos=self.head['checksumPos']
      for pos in [self.head['urlPtrPos'], self.head['titlePtrPos'], self.head['mimeListPos'], self.head['clusterPtrPos'], min(self.urlPtrList)]:
        if pos>self.clustersStarts[0] and pos<nextEltPos:
          nextEltPos=pos
      return nextEltPos

    
    def parseClusters(self):
        self.zim.seek(self.head['clusterPtrPos'])
        self.clustersStarts=struct.unpack('Q'*self.head['clusterCount'], self.zim.read(8*self.head['clusterCount']))
        self.clustersLengths=[-1]*len(self.clustersStarts)
        pos=0

        for pos in range(len(self.clustersStarts)):
            if pos >0:
                self.clustersLengths[pos-1]=self.clustersStarts[pos]-self.clustersStarts[pos-1]
            
        
        lastElt=len(self.clustersStarts)-1
        self.clustersLengths[lastElt]=self.offsetAfterClusters()-self.clustersStarts[lastElt]


class iterArticlesZim:
    def __init__(self, parseZim):
        self.parseZim=parseZim
        self.pos=0        
        self.parseZim.zim.seek(self.parseZim.urlPtrList[0]);

    def __iter__(self):
        return self

    def __next__(self): # Python 3: def __next__(self)
        if self.pos >= self.parseZim.head['articleCount']:
            raise StopIteration
        else:
            self.pos += 1
            if (self.pos%25000==0):
                print(str(self.pos)+' entries readen out of '+str(self.parseZim.head['articleCount']))
            return self.parseZim.readEntry()
  
  def copyData(inFile, outFile, inOffset, outOffset, dataLen):
    MEGA=1048576
    
    inFile.seek(inOffset)
    outFile.seek(outOffset)
    
    lenCopied=0
    while lenCopied<dataLen:
        lenToCopy=min(MEGA, dataLen-lenCopied)
        outFile.write(inFile.read(lenToCopy))
        lenCopied+=lenToCopy

class cluster:
    
    def __init__(self, infoByte, content):
        #nothing
        self.infoByte=infoByte
        self.compression=self.infoByte&0x0f
        offset=(self.infoByte&0xf0) >> 4
        self.blobs=[]
        
        if self.compression == 4:
            content=lzma.decompress(content)
        if self.compression == 5:
          subprocess.run(["zstd", "-dcf", "-o", "/tmp/decompress"], input=content, stderr=subprocess.DEVNULL) 
          f=open("/tmp/decompress", "rb")
          content=f.read()
          f.close()
          subprocess.run(["rm", "/tmp/decompress"])


        if offset > 0 :
            self.offSize=8
            self.strFormat="Q"
        else:
            self.offSize=4
            self.strFormat="I"

        self.readBlobs(content)
        

    
    def readBlobs(self, content):
        headerSize=struct.unpack(self.strFormat, content[:self.offSize])[0]
        
        idx=0
        while idx*self.offSize<headerSize:
            blobStart=struct.unpack(self.strFormat, content[idx*self.offSize:(idx+1)*self.offSize])[0]
            if blobStart>=len(content):
                break
            blobEnd=struct.unpack(self.strFormat, content[(idx+1)*self.offSize:(idx+2)*self.offSize])[0]
            
            self.blobs.append(content[blobStart:blobEnd])
            
            idx+=1
        

    def write(self):
            #computing the blob header
            dataOffset=(len(self.blobs)+1)*self.offSize #+1 because last element points to the end of the blob
            arBlobStarts=[dataOffset+0]
            actOffset=dataOffset
            for blob in self.blobs:
                arBlobStarts.append(actOffset+len(blob))
                actOffset+=len(blob)
    
            clusterContent=struct.pack(self.strFormat*len(arBlobStarts), *arBlobStarts)
            
            for blob in self.blobs:
                clusterContent+=blob
    
            if self.compression == 4:
                clusterContent=lzma.compress(clusterContent)
            if self.compression == 5:
                clusterContent=zstd.compress(clusterContent) #not tested due that decompression using thos methos doesn't works......

            
            clusterInfoByte=struct.pack('B', self.infoByte)
            
            return clusterInfoByte+clusterContent
      
    def getCompression(self):
      return self.compression
    
    def changeCompression(self, newCompress):
      self.compression=newCompress
      self.infoByte=(self.infoByte&0xf0) | self.compression
      

    def getBlobContent(self, blobId):
        return self.blobs[blobId].decode()
    
    def updateBlobContent(self, blobId, content):
        self.blobs[blobId]=content.encode()



class DerivatedZim:
    def __init__(self, inZim, outZim, parseInZim):
        self.inZim=inZim
        self.outZim=outZim
        self.parseInZim=parseInZim
        self.head=self.parseInZim.head.copy()
        
        self.listClusterAlter=[] #list of cluster to be altered
        self.prmsClusterAlter=[] #for each cluster to be altered, an array of array [blobId, replacement]
        
        self.clustersStarts=[]
        self.actClusterWriteOffset=self.parseInZim.clustersStarts[0]
        
        self.wantedClusters=range(len(self.parseInZim.clustersStarts)) #by default, set we want all clusters 
        
        self.copyStructureDataBeforeClusters()
        
        self.convertCompress=False
    
    def copyStructureDataBeforeClusters(self):
        lenCopy=self.parseInZim.clustersStarts[0]
        copyData(self.inZim, self.outZim, 0, 0, lenCopy)

    def copyStructureDataAfterClusters(self):
      inStartOffset=self.parseInZim.offsetAfterClusters()
      outStartOffset=self.actClusterWriteOffset
      self.offsetDelta=outStartOffset-inStartOffset
      lenCopy=self.parseInZim.head['checksumPos']-inStartOffset
      
      
      if(lenCopy>0):
        #print("i="+str(inStartOffset)+"     o="+str(outStartOffset)+"    l="+str(lenCopy)+"    delta="+str(self.offsetDelta))
        copyData(self.inZim, self.outZim, inStartOffset, outStartOffset, lenCopy)
      
      if(self.head['urlPtrPos']>=inStartOffset):
        self.head['urlPtrPos']+=self.offsetDelta
      if(self.head['titlePtrPos']>=inStartOffset):
        self.head['titlePtrPos']+=self.offsetDelta
      if(self.head['clusterPtrPos']>=inStartOffset):
        self.head['clusterPtrPos']+=self.offsetDelta
      if(self.head['mimeListPos']>=inStartOffset):
        self.head['mimeListPos']+=self.offsetDelta
      self.head['checksumPos']+=self.offsetDelta
      self.writeHead()
         
      for i in range(self.head['articleCount']):
        self.outZim.seek(self.head['urlPtrPos']+i*8)
        articlePos=struct.unpack('Q', self.outZim.read(8))[0]
        if articlePos >= inStartOffset:
          articlePos+=self.offsetDelta
          self.outZim.seek(self.head['urlPtrPos']+i*8)
          self.outZim.write(struct.pack('Q', articlePos))

    def copyCluster(self, clusterId):
        lenCopy=self.parseInZim.clustersLengths[clusterId]
        copyData(self.inZim, self.outZim, self.parseInZim.clustersStarts[clusterId], self.actClusterWriteOffset, lenCopy)
        self.clustersStarts.append(self.actClusterWriteOffset)
        self.actClusterWriteOffset+=lenCopy
      
    def writeEmptyCluster(self, clusterId):
        self.outZim.seek(self.actClusterWriteOffset)
        self.outZim.write(struct.pack("=BIIB", 0, 8, 9, 0)) #write a one cluster with only one bloc on one byte.
        self.clustersStarts.append(self.actClusterWriteOffset)
        self.actClusterWriteOffset+=10
 
    def updateClusterPtrs(self):
        self.outZim.seek(self.head['clusterPtrPos'])
        self.outZim.write(struct.pack('Q'*len(self.clustersStarts), *self.clustersStarts))

    def processCopy(self):
        count=0
        for cluster in range(len(self.parseInZim.clustersStarts)):
            if cluster in self.wantedClusters:
                if cluster in self.listClusterAlter or self.convertCompress is not False:
                    self.writeUpdatedCluster(cluster)
                else:
                    self.copyCluster(cluster)
            else:
                self.writeEmptyCluster(cluster)
            count+=1
            if count%300 == 0:
                print(str(count)+" clusters copied out of "+str(self.head['clusterCount']))
    
        self.copyStructureDataAfterClusters()
        self.updateClusterPtrs()
        
    def deleteEntry(self, nameSpace, url):
      originalEntry=self.parseInZim.findEntry(nameSpace, url)
      pos=originalEntry['posInFile']+self.offsetDelta
      
      if originalEntry['mimeType']==ParseZim.MIME_REDIRECT:
        padding=4
      elif  originalEntry['mimeType']==ParseZim.MIME_LINKTARGET or originalEntry['mimeType']==ParseZim.MIME_DELETED:
        padding=0
      else:
        padding=8

      self.outZim.seek(pos)
      self.outZim.write(struct.pack('H', ParseZim.MIME_DELETED)) 
      self.outZim.write(struct.pack('B', originalEntry['parameterLen']+padding)) 
      self.outZim.write('!'.encode()) #seems to work only if moving to another namespace
      self.outZim.write(struct.pack('I', originalEntry['revision']))
      self.outZim.write(originalEntry['url'].encode()+b'\x00')
      self.outZim.write(originalEntry['title'].encode()+b'\x00')


    def writeUpdatedCluster(self, clusterId):
        self.inZim.seek(self.parseInZim.clustersStarts[clusterId])
        infoByte=struct.unpack('B', self.inZim.read(1))[0]
        clusterContent=self.inZim.read(self.parseInZim.clustersLengths[clusterId]-1)
        objCluster=cluster(infoByte, clusterContent)
        del clusterContent
        
        try:
          idx=self.listClusterAlter.index(clusterId)
          for prmsUpdt in self.prmsClusterAlter[idx]:
              blobId=prmsUpdt[0]
              repl=prmsUpdt[1]
              
              if callable(repl):
                  objCluster.updateBlobContent(blobId, repl(objCluster.getBlobContent(blobId)))
              else:
                  objCluster.updateBlobContent(blobId, repl)
        except ValueError:
          pass
        
        if self.convertCompress is not False and self.convertCompress!=objCluster.getCompression() :
          objCluster.changeCompression(self.convertCompress)
        

        toWrite=objCluster.write()
        
        self.clustersStarts.append(self.actClusterWriteOffset)
        self.outZim.seek(self.actClusterWriteOffset)
        self.outZim.write(toWrite)
        self.actClusterWriteOffset+=len(toWrite)

        def registerArticleUpdate(self, nameSpace, url, repl):
        article=self.parseInZim.findEntry(nameSpace, url)
        if article == False:
          return False
        
        newPrmTuple=[article['blob'], repl]
        if article['cluster'] in self.listClusterAlter:
          idx=self.listClusterAlter.index(article['cluster'])
          self.prmsClusterAlter[idx].append(newPrmTuple)
        else:
          self.listClusterAlter.append(article['cluster'])
          self.prmsClusterAlter.append([newPrmTuple])
            
    def updateChecksum(self):
      block_size=2**20

      checksumPos=self.head['checksumPos']
      self.outZim.seek(72); #go to position of checksumpos in header
      self.outZim.write(struct.pack('Q', checksumPos))

      md5 = hashlib.md5()
      self.outZim.seek(0)
      while True:
        data = self.outZim.read(block_size)
        if not data:
          break
        md5.update(data)
      
      if (self.outZim.tell()!=checksumPos):
        print("incorect position after checksum calc!!! Something went wrong...") #this should never happens
      self.outZim.write(md5.digest())
    
    def writeHead(self):
      self.outZim.seek(0)
      self.outZim.write(struct.pack('I', self.head['magicNumber']))
      self.outZim.write(struct.pack('H', self.head['majorVersion']))
      self.outZim.write(struct.pack('H', self.head['minorVersion']))
      self.outZim.write(struct.pack('Q', self.head['uuidA']))
      self.outZim.write(struct.pack('Q', self.head['uuidB']))
      self.outZim.write(struct.pack('I', self.head['articleCount']))
      self.outZim.write(struct.pack('I', self.head['clusterCount']))
      self.outZim.write(struct.pack('Q', self.head['urlPtrPos']))
      self.outZim.write(struct.pack('Q', self.head['titlePtrPos']))
      self.outZim.write(struct.pack('Q', self.head['clusterPtrPos']))
      self.outZim.write(struct.pack('Q', self.head['mimeListPos']))
      self.outZim.write(struct.pack('I', self.head['mainPage']))
      self.outZim.write(struct.pack('I', self.head['layoutPage']))
      self.outZim.write(struct.pack('Q', self.head['checksumPos']))


