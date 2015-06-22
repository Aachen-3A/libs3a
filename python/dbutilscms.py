#!/usr/bin/env python
## @package dbutilscms
# This module provides classes to work with various cms databases
#
# This module provides classes to work with various cms databases
# Currently supported databases: McM, DBS

# mcm utilities imports
import urllib2
import json
import logging

log = logging.getLogger( 'dbutilscms' )

class McMUtilities():
    def __init__(self):
        self.mcm_prefix = "https://cms-pdmv.cern.ch/mcm/public/restapi/requests/produces/"
        self.mcm_json = None # requested json
        self.gen_json = None # gen-sim json


    def readJSON(self, json_string):
        try:
            return json.load(json_string)
        except ValueError:
            log.error("Could not read JSON at request URL.")
            return None


    def readURL(self, mcm_dataset):
        try:
            self.mcm_json = self.readJSON(urllib2.urlopen(self.mcm_prefix + mcm_dataset))
        except urllib2.HTTPError:
            log.error("Could not find dataset " + mcm_dataset)
            self.mcm_json = None

        # reset gen-sim json
        self.gen_json = None


    # generator level information wrapper
    def getGenInfo(self, key):
        # use gen-sim json if it exists, else start with requested json
        tmp_json = self.gen_json if self.gen_json else self.mcm_json

        if len(tmp_json["results"]) is 0:
            log.error("JSON file is empty. Wrong URL?")
            return None

        # check if sample has generator parameters entry
        while len(tmp_json["results"]["generator_parameters"]) is 0:
            # if not, find parent dataset
            input_dataset = tmp_json["results"].get("input_dataset", None)
            if input_dataset is not None:
                tmp_json = self.readJSON(urllib2.urlopen(self.mcm_prefix + input_dataset))
            else:
                log.error("No input dataset specified in JSON. Cannot find GEN-SIM sample.")
                return None

        # store gen json for quicker access upon next call
        self.gen_json = tmp_json

        # return value
        # find position of dict in list
        for dictcand in tmp_json["results"]["generator_parameters"]:
            try:
                value = dictcand.get(key, None)
            except:
                value = None
        if value is None:
            log.error("Could not retrieve " + key + ".")
        return value


    def getCrossSection(self):
        return self.getGenInfo("cross_section")


    def getFilterEfficiency(self):
        return self.getGenInfo("filter_efficiency")


    # sample level information wrapper
    def getInfo(self, key):
        value = self.mcm_json["results"].get(key, None)
        if value is None:
            log.error("Could not retrieve " + key + ".")
        return value


    def getEvents(self):
        return self.getInfo("total_events")


    def getGenerators(self):
        return self.getInfo("generators")[0]


    def getEnergy(self):
        return self.getInfo("energy")


    def getCMSSW(self):
        return self.getInfo("cmssw_release")


    def getWorkingGroup(self):
        return self.getInfo("pwg")

import das_client
## The dasClientHelper Class
#
# This is a helper class for the das_client cli
#
class dasClientHelper():
    ## The constructor.
    # @param self: The object pointer.
    def __init__(self):
        # get all default options for queries
        init_parser = das_client.DASOptionParser()
        # we can not use the class memebr function
        # get_opt because it may lead to conflicts with other
        # parsers
        self.opts , _= init_parser.parser.parse_args([' '])
        # we get all as default
        self.opts.limit = 0
        self.datasetJSON = None

    ## Reimplementation of get_data from das_client
    #
    # This function is used to send a general query to das
    # The query is seperated i 3 parts (see arguments)
    # @param self: The object pointer.
    # @param dataset: String containing the dataset name
    # @param queryobject: String which specifies the object you want to query (file, block etc.)
    # @param query_aggregation: additional aggregation query parts at the end.
    # @return json dictionary containing the das query response
    def get_data(self, dataset, queryobject = None, query_aggregation=None):
        if queryobject is not None:
            query = queryobject + " "
        else: query = ''
        query += "dataset=%s " % dataset
        if query_aggregation is not None:
            query += " | %s" % query_aggregation
        jsondict = das_client.get_data( self.opts.host,
                                        query,
                                        self.opts.idx,
                                        self.opts.limit,
                                        self.opts.verbose,
                                        self.opts.threshold,
                                        self.opts.ckey,
                                        self.opts.cert)
        return jsondict
    ## Get a dict containing most common dataset infos
    #
    # @param dataset: String containing the dataset name
    # @return A dictionary containing the infos: name, nevents, nfiles, nlumis, nblocks, size (byte)
    def getDatasetSummary( self, dataset):
        if self.datasetJSON is None:
            jsondict = self.get_data( dataset )
            self.datasetJSON = jsondict
        summary = self.datasetJSON['data'][0]['dataset'][1]
        for infodict in self.datasetJSON['data'][0]['dataset']:
            if 'nlumis' in infodict.keys():
                summary = infodict
            if 'acquisition_era_name' in infodict.keys():
                extrainfos = infodict
        summary['acquisition_era_name'] = extrainfos['acquisition_era_name']
        summary['datatype'] = extrainfos['datatype']
        return summary
