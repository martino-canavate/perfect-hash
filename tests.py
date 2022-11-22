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





class testFixedHits(unittest.TestCase):

    index_build.CorpusFiles = {'CAA': 'Carroll_Alice_in_Wonderland.txt',

                               'DCC': 'Dickens_Christmas_Carol.txt',

                               'SJH': 'Stevenson_Jekyll_and_Hyde.txt',

                               'SCW': 'Shakespeare_Complete_Works.txt',

                               'TWP': 'Tolstoy_War_and_Peace.txt',

                               }

    index_build.buildIndex()

    index_build.generateMetaIndex('index.txt')



    def test_correct_hits(self):

        index_build.generateMetaIndex('index.txt')

        line_window = 1

        keys = ['exit', 'pursued']

        itemStreams = getItemstreams(keys)



        min_required = 2

        hits = search_queries.HitStream(itemStreams, line_window, min_required)

        expected = [("SCW", 80960), ("SCW", 158724)]

        count = 0

        a = hits.next()

        while a != None:

            self.assertEqual(a, expected[count])

            count += 1

            a = hits.next()

        self.assertEqual(count, len(expected))



    def test_correct_hits_advanced(self):

        keys = ['romans', 'friends', 'countrymen']

        line_window = 5

        min_required = 2

        itemStreams = getItemstreams(keys)

        hits = search_queries.HitStream(itemStreams, line_window, min_required)



        expected = [("SCW", 18636), ("SCW", 18637), ("SCW", 45457), ("SCW", 49237), ("SCW", 49579), ("SCW", 65596),

                    ("SCW", 65751), ("SCW", 66529),

                    ("SCW", 66852), ("SCW", 66965), ("SCW", 68747), ("SCW", 68748), ("SCW", 87466), ("SCW", 112532),

                    ("SCW", 125458), ("SCW", 136064), ("SCW", 136072)]

        count = 0

        a = hits.next()

        while a is not None:

            self.assertEqual(a, expected[count])

            count += 1

            a = hits.next()

        self.assertEqual(count, len(expected))

