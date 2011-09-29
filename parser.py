'''
Created on Sep 29, 2011

@author: kykamath
'''
from lxml import etree
import dateutil.parser

current_data_path = 'android_enthusiasts/'

def iterateRows(file, interstedKeys=[]):
    for line in open(file):
        if line.strip().startswith('<row'): 
            dataToYield = dict([(k, etree.fromstring(line).attrib[k]) for k in interstedKeys if k in etree.fromstring(line).attrib])
            for k in dataToYield:
                if k=='CreationDate': dataToYield[k] =  dateutil.parser.parse(dataToYield[k])
                elif k.endswith('Count') or k.endswith('Id'): 
                    if dataToYield[k]!='':dataToYield[k]=int(dataToYield[k])
                    else: dataToYield[k]=0
                elif k=='Tags': dataToYield[k] = dataToYield[k][1:-1].split('><')
            yield dataToYield
            
def iteratePosts(data_path, interstedKeys = 'Id PostTypeId ParentID CreationDate ViewCount OwnerUserId Tags AnswerCount CommentCount FavoriteCount'.split()):
    for l in iterateRows(data_path+'posts.xml', interstedKeys): yield l
def iterateComments(data_path, interstedKeys = 'Id PostId CreationDate UserId'.split()):
    for l in iterateRows(data_path+'comments.xml', interstedKeys): yield l

i = 1
for l in iterateComments(current_data_path):
    print i, l
    i+=1
