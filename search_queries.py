
# Inf2-IADS Coursework 1, October 2020
# Python source file: search_queries.py
# Author: John Longley

# TEMPLATE FILE
# Please add your code at the point marked: # TODO


# PART B: PROCESSING SEARCH QUERIES

import index_build
from index_build import *
index_build.generateMetaIndex('index.txt')

# We find hits for queries using the index entries for the search terms.
# Since index entries for common words may be large, we don't want to
# process the entire index entry before commencing a search.
# Instead, we process the index entry as a stream of items, each of which
# references an occurrence of the search term.

# For example, the (short) index entry

#    'ABC01,23,DEF004,056,789\n'

# generates a stream which successively yields the items

#    ('ABC',1), ('ABC',23), ('DEF',4), ('DEF',56), ('DEF',789), None, None, ...

# Item streams also support peeking at the next item without advancing.

class ItemStream:
    def __init__(self,entryString):
        self.entryString = entryString
        self.pos = 0
        self.doc = 0
        self.comma = 0
    def updateDoc(self):
        if self.entryString[self.pos].isalpha():
            self.doc = self.entryString[self.pos:self.pos+3]
            self.pos += 3
    def peek(self):
        if self.pos < len(self.entryString):
            self.updateDoc()
            self.comma = self.entryString.find(',',self.pos)
                    # yields -1 if no more commas after pos
            line = int(self.entryString[self.pos:self.comma])
                    # magically works even when comma == -1, thanks to \n
            return (self.doc,line)
        # else will return None
    def pop(self):
        e = self.peek()
        if self.comma == -1:
            self.pos = len(self.entryString)
        else:
            self.pos = self.comma + 1
        return e

# TODO
# Add your code here.
# classical sorts taught in class, but this one sorts Item streams
def mergeStream(B,C):
    D = [0] * (len(B)+len(C))
    i,j = 0,0
    for k in range(0,len(D)):
        if (i < len(B) and (j == len(C) or B[i].peek() < C[j].peek())):
            D[k] = B[i]
            i += 1
        else:
            D[k] = C[j]
            j += 1
    return D

# classical mergeSort taught in class
def mergeSortStream(A,m,n):
    if n-m == 1:
        return [A[m]]
    else:
        p = (m+n)//2
        B = mergeSortStream(A,m,p)
        C = mergeSortStream(A,p,n)
        return mergeStream(B,C)


class HitStream:
    def __init__(self, itemStreams, lineWindow, minRequired):
        self.itemStreams = itemStreams
        self.lineWindow = lineWindow
        self.minRequired = minRequired
    
    #this function avoids double counting hits, and at the same time drops the smallest element
    def repetitionChecker(self, stream , checked):
        while checked == stream.peek():
            stream.pop()


    def getSortedTuples(self):
        #checks if stream is finished
        for x in self.itemStreams:
            if x.peek() is None:
                self.itemStreams.remove(x)
        #sorts remaining hits
        self.itemStreams = mergeSortStream(self.itemStreams, 0, len(self.itemStreams))
        #checks if it's still possible to get hits with the number of streams available
        if len(self.itemStreams) < self.minRequired:
            self.itemStreams = None


    def next(self):
        #keeps loop alive while ther is a possibility of hitting
        while len(self.itemStreams) >= self.minRequired:
            self.getSortedTuples()
            #checks if all streams are finished
            if self.itemStreams is None:
                return None
            checkingMinima = self.itemStreams[0].peek()
            hits = 0
            #checks hits with the minimum value
            for x in self.itemStreams:
                checkingHit = x.peek()
                if checkingMinima[1] == checkingHit[1] and checkingMinima[0] == checkingHit[0]:
                    self.repetitionChecker(x, checkingMinima)
                    hits += 1
                elif ((checkingMinima[1] + self.lineWindow -1) >= checkingHit[1]) and (checkingMinima[0] == checkingHit[0]):
                    hits += 1
                #breaks the cycle when the remaining streams are out of range of line window
                else:
                    break
            #checks if minRequired is achieved
            if hits >= self.minRequired:
                return(checkingMinima)
        return None





# Displaying hits as corpus quotations:

import linecache

def displayLines(startref,lineWindow):
    # global CorpusFiles
    if startref is not None:
        doc = startref[0]
        docfile = index_build.CorpusFiles[doc]
        line = startref[1]
        print ((doc + ' ' + str(line)).ljust(16) +
               linecache.getline(docfile,line).strip())
        for i in range(1,lineWindow):
            print (' '*16 + linecache.getline(docfile,line+i).strip())
        print ('')

def displayHits(hitStream,numberOfHits,lineWindow):
    for i in range(0,numberOfHits):
        startref = hitStream.next()
        if startref is None:
            print('-'*16)
            break
        displayLines(startref,lineWindow)
    linecache.clearcache()
    return hitStream


# Putting it all together:

currHitStream = None

currLineWindow = 0

def advancedSearch(keys,lineWindow,minRequired,numberOfHits=5):
    indexEntries = [index_build.indexEntryFor(k) for k in keys]
    if not all(indexEntries):
        message = "Words absent from index:  "
        for i in range(0,len(keys)):
            if indexEntries[i] is None:
                message += (keys[i] + " ")
        print(message + '\n')
    itemStreams = [ItemStream(e) for e in indexEntries if e is not None]
    if len(itemStreams) >= minRequired:
        global currHitStream, currLineWindow
        currHitStream = HitStream (itemStreams,lineWindow,minRequired)
        currLineWindow = lineWindow
        displayHits(currHitStream,numberOfHits,lineWindow)

def easySearch(keys,numberOfHits=5):
    global currHitStream, currLineWindow
    advancedSearch(keys,1,len(keys),numberOfHits)

def more(numberOfHits=5):
    global currHitStream, currLineWindow
    displayHits(currHitStream,numberOfHits,currLineWindow)

#easySearch(['pursued', 'exit'])
#easySearch(['palpable', 'very'])
#advancedSearch(['friends','romans','countrymen'],5,2,20)
# End of file
