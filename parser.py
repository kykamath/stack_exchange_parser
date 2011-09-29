'''
Created on Sep 29, 2011

@author: kykamath
'''
from lxml import etree
import dateutil.parser, os, cjson, glob
from library.file_io import FileIO

current_data_path = '/mnt/chevron/kykamath/data/stack_exchange/Content/android_enthusiasts/'
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
        nextIteratorWithSmallestValue = min(iterators, key=lambda it: dateutil.parser.parse(it.current['CreationDate']))
        yield nextIteratorWithSmallestValue.current
        if nextIteratorWithSmallestValue.setCurrent(): iterators.remove(nextIteratorWithSmallestValue)
        
def sortFile(fileName):
    dataToWrite = sorted(FileIO.iterateJsonFromFile(fileName), key=lambda l: dateutil.parser.parse(l['CreationDate']))
    print 'Sorting file', fileName
    f = open('%s_sorted'%fileName, 'w')
    for line in dataToWrite: f.write(cjson.encode(line)+'\n')
    os.system('mv %s_sorted %s'%(fileName, fileName))
    f.close()

def createOutputFileFor(current_data_path, iterators):
    numberOfSplit = 10
    totalLines = 0
    tempDir = '%s/temp/'%current_data_path
    allDataFile = tempDir+'data.txt'
    if not os.path.exists(tempDir):  os.umask(0), os.makedirs(tempDir, 0777)
    for iterator in iterators:
        for data in iterator: 
            totalLines+=1
            FileIO.writeToFileAsJson(data, allDataFile)
    linesPerFile = totalLines/numberOfSplit
    os.chdir(tempDir)
    os.system('split -l %s %s'%(linesPerFile, allDataFile))

    [sortFile(file) for file in glob.glob( os.path.join(tempDir, '*') ) if file!=allDataFile]
    
    i = 1
    for data in iterateDataOrderedByTime([Iterator(FileIO.iterateJsonFromFile(file)) for file in glob.glob( os.path.join(tempDir, '*')) if file!=allDataFile]):
        print i, data['RowType'], data['CreationDate']
        i+=1

createOutputFileFor(current_data_path, [iteratePosts(current_data_path), iterateComments(current_data_path)])

#sortFile('/mnt/chevron/kykamath/data/stack_exchange/Content/android_enthusiasts/temp/xaa')

#i = 1
#for data in iterateDataOrderedByTime([Iterator(iterateComments(current_data_path)), Iterator(iteratePosts(current_data_path))]):
#    print i, data['RowType'], data['CreationDate']
#    i+=1
