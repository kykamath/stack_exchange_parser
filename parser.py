'''
Created on Sep 29, 2011

@author: kykamath
'''
from lxml import etree
import dateutil.parser, os
from library.file_io import FileIO

current_data_path = '/mnt/chevron/kykamath/data/stack_exchange/Content/android_enthusiasts'
#current_data_path = '/mnt/chevron/kykamath/data/stack_exchange/Content/server_fault/'
POST='post'
COMMENT='comment'

def iterateRows(file, rowType, interstedKeys=[]):
    for line in open(file):
        if line.strip().startswith('<row'): 
            dataToYield = dict([(k, etree.fromstring(line).attrib[k]) for k in interstedKeys if k in etree.fromstring(line).attrib])
            for k in dataToYield:
#                if k=='CreationDate': dataToYield[k] =  dateutil.parser.parse(dataToYield[k])
                if k.endswith('Count') or k.endswith('Id'): 
                    if dataToYield[k]!='':dataToYield[k]=int(dataToYield[k])
                    else: dataToYield[k]=0
                elif k=='Tags': dataToYield[k] = dataToYield[k][1:-1].split('><')
            dataToYield['RowType'] = rowType
            yield dataToYield
            
def iteratePosts(data_path, rowType=POST, interstedKeys = 'Id PostTypeId ParentID CreationDate ViewCount OwnerUserId Tags AnswerCount CommentCount FavoriteCount'.split()):
    for l in iterateRows(data_path+'posts.xml', rowType, interstedKeys): yield l

def iterateComments(data_path, rowType=COMMENT,interstedKeys = 'Id PostId CreationDate UserId'.split()):
    for l in iterateRows(data_path+'comments.xml', rowType, interstedKeys): yield l
    
def createOutputFileFor(current_data_path, iterators):
    numberOfSplit = 10
    totalLines = 0
    tempDir = '%s/temp'%current_data_path
    allDataFile = tempDir+'data.txt'
    os.umask(0), os.makedirs(tempDir, 0777)
    for iterator in iterators:
        for data in iterator: 
            totalLines+=1
            FileIO.writeToFileAsJson(data, allDataFile)
    linesPerFile = totalLines/numberOfSplit
    os.system('cd %s'%tempDir)
    os.system('split -l %s %s'%(linesPerFile, allDataFile))
    
    

class Iterator:
        def __init__(self, iterator): 
            self.iterator, self.empty = iterator, False
            self.setCurrent()
        def setCurrent(self): 
            try:
                self.current = self.iterator.next()
            except StopIteration: self.empty = True
            return self.empty

def iterateDataOrderedByTime(iterators):
    while len(iterators)>0:
        nextIteratorWithSmallestValue = min(iterators, key=lambda it: it.current['CreationDate'])
        yield nextIteratorWithSmallestValue.current
        if nextIteratorWithSmallestValue.setCurrent(): iterators.remove(nextIteratorWithSmallestValue)

createOutputFileFor(current_data_path, [iteratePosts(current_data_path), iterateComments(current_data_path)])

#i = 1
#for data in iterateDataOrderedByTime([Iterator(iterateComments(current_data_path)), Iterator(iteratePosts(current_data_path))]):
#    print i, data['RowType'], data['CreationDate']
#    i+=1
