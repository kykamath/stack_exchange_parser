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
            if 'CreationDate' in dataToYield: dataToYield['CreationDate'] =  dateutil.parser.parse(dataToYield['CreationDate'])
            yield dataToYield
            
def iteratePosts(data_path, interstedKeys = 'Id PostTypeId ParentID CreationDate ViewCount OwnerUserId Tags AnswerCount CommentCount FavoriteCount'.split()):
    for l in iterateRows(data_path+'posts.xml', interstedKeys): yield l

i = 1
for l in iteratePosts(current_data_path):
    print i, l
    i+=1
#import dateutil.parser
#s='2008-09-06T08:07:10.730'
#print type(dateutil.parser.parse(s))
    