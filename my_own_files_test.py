import unittest
import index_build
import search_queries
import os


def getItemstreams(keys):
    indexEntries = [index_build.indexEntryFor(k) for k in keys]
    if not all(indexEntries):
        message = "Words absent from index:  "
        for i in range(0, len(keys)):
            if indexEntries[i] is None:
                message += (keys[i] + " ")
        print(message + '\n')
    itemStreams = [search_queries.ItemStream(e) for e in indexEntries if e is not None]
    return itemStreams


class myOwnHits(unittest.TestCase):
    index_build.CorpusFiles = CorpusFiles = {
        'ZZZ': 'test/output_merge.txt',
        'TES': 'test/repeated_key_in_line.txt',
        'TRY': 'test/test.txt',
        'LOR': 'test/lorem_ipsum.txt',

    }
    index_build.buildIndex()
    index_build.generateMetaIndex('index.txt')

    def test_my_search(self):
        keys = ['aaaa', 'bbbb']
        line_window = 2
        min_required = 2
        itemStreams = getItemstreams(keys)
        hits = search_queries.HitStream(itemStreams, line_window, min_required)
        expected = [('ZZZ', 945)]
        count = 0
        a = hits.next()
        while a != None:
            self.assertEqual(a, expected[count])
            count += 1
            a = hits.next()
        self.assertEqual(count, len(expected))


    def test_my_search_2(self):
            keys = ['aaaa', 'bbbb', 'cccc']
            line_window = 2
            min_required = 2
            itemStreams = getItemstreams(keys)
            hits = search_queries.HitStream(itemStreams, line_window, min_required)
            expected = [('ZZZ', 945), ('ZZZ', 952)]
            count = 0
            a = hits.next()
            while a != None:
                self.assertEqual(a, expected[count])
                count += 1
                a = hits.next()
            self.assertEqual(count,len(expected))

    def test_repeated_key(self):
        keys = ['estoyrepetida', 'hola']
        line_window = 2
        min_required = 2
        itemStreams = getItemstreams(keys)
        hits = search_queries.HitStream(itemStreams, line_window, min_required)

        expected = [('TES', 1)]
        count = 0
        a = hits.next()
        while a != None:
            self.assertEqual(a, expected[count])
            count += 1
            a = hits.next()
        self.assertEqual(count, len(expected))


    def test_my_search_3(self):
            keys = ['distance', 'determine']
            line_window = 2
            min_required = 2
            itemStreams = getItemstreams(keys)
            hits = search_queries.HitStream(itemStreams, line_window, min_required)
            expected = [('TRY', 7), ('TRY', 23), ('TRY', 24), ]
            count = 0
            a = hits.next()
            while a != None:
                self.assertEqual(a, expected[count])
                count += 1
                a = hits.next()
            self.assertEqual(count, len(expected))


    def test_number_of_hits_one_word(self):
            keys = ['lorem']
            min_required = 1
            line_window = 1
            itemStreams = getItemstreams(keys)
            hits = search_queries.HitStream(itemStreams, line_window, min_required)
            expected = 46
            count = 0
            while hits.next()!= None:
                count += 1
            self.assertEqual(count, expected)


    def test_my_search_4(self):
        keys = ['lorem','viverra']
        min_required = 2
        line_window = 1
        itemStreams = getItemstreams(keys)
        hits = search_queries.HitStream(itemStreams, line_window, min_required)
        expected = [("LOR",3),("LOR",9),("LOR",61),("LOR",71),("LOR",75),("LOR",77),("LOR",89),("LOR",95),("LOR",99),("LOR",101),("LOR",123),("LOR",151),("LOR",159),("LOR",171),("LOR",197)]
        a = hits.next()
        count = 0
        while a != None:
            self.assertEqual(a, expected[count])
            count += 1
            a = hits.next()
        self.assertEqual(count, len(expected))

    def test_exception(self):
        try:

            index_build.buildIndex()
        except:
            self.fail("You are Juanpi")



