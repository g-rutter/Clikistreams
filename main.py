from pyspark import SparkContext, SparkConf
import codecs
import os

def SankeyFactory(clickstreams, page, out_dir):

    in_file = "Sankey_Google_template.html"
    out_file = "Sankey_"+page[:-17]+".html"

    with open(in_file, 'r') as template:
        sankey = template.readlines()

    prefix = ""
    # Create entry for each stream. clickstreams is divided into lists of in
    # and out of the disambiguation page.
    for clickstream in clickstreams:
        for click in clickstream:
            new_entry = "       " + \
                        "[ \'" + click[1] + "\', \'" + prefix + click[2] +\
                        "\', " + str(click[0]) + " ],\n"
            sankey.insert(16, new_entry)

        prefix = " "

    with codecs.open(out_dir + '/' + out_file, 'w', encoding="utf-8") as sankeyfile:
        sankeyfile.write(u"".join(sankey))

if __name__ == "__main__":

    ############
    #  Set-up  #
    ############

    sc = SparkContext()
    source = "2015_2_clickstream.tsv"
    textFile = sc.textFile(source, use_unicode=True)
    out_dir = 'sankey_selection'
    n_disamb = 100 # Number of top disambigation pages to find

    try:
        os.mkdir(out_dir)
    except OSError:
        pass

    #####################
    #  Data processing  #
    #####################

    # Prepare smaller database which all future operations can start from
    # Disambig entry has fields: count, prev_title, curr_title
    disambig = textFile.map(lambda line: line.split("\t")[2:5])\
                       .filter(lambda entry: "disambiguation" in entry[1] or
                                             "disambiguation" in entry[2])\
                       .map(lambda entry: [int(entry[0])] +
                                          [entry[1].replace("_", " ")\
                                                   .replace("'", "\\'")] +
                                          [entry[2].replace("_", " ")\
                                                   .replace("'", "\\'")])

    disambig.persist()

    # Get top returned-to disambiguation pages
    top_returned = disambig.filter(lambda entry: "disambiguation" in entry[2] and
                                                    "other-" != entry[1][:6])\
                           .map(lambda entry: (entry[2], entry[0]))\
                           .reduceByKey(lambda x, y: x + y)\
                           .takeOrdered(n_disamb, key=lambda entry: -entry[1])

    ##################
    #  Make Sankeys  #
    ##################

    for disambig_page, _ in top_returned:

        # Grab arrays of streams starting or ending with the disambiguation page
        clicks_out = disambig.filter(lambda entry: (disambig_page == entry[1])
                                                   and (disambig_page != entry[2]))\
                             .collect()
        clicks_in = disambig.filter(lambda entry: (disambig_page == entry[2])
                                                  and (disambig_page != entry[1]))\
                               .collect()

        # Make Sankey HTML file
        SankeyFactory([clicks_in, clicks_out], disambig_page, out_dir)
