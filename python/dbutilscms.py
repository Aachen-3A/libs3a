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
# dbs imports
from dbs.apis.dbsClient import DbsApi

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

## The DBSUtilities Class
#
# This is a helper class for the dbs database

class DBSUtilities():
    
    ## The constructor.
    # @param self: The object pointer.
    def __init__(self):
        self.dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.dbsApi = DbsApi( url = self.dbsUrl )
        self.numEvents = 0
        self.numFiles = 0
        self.totalFileSize = 0
    
    ## Function to get all detail blocks for a dataset
    # @param self: The object pointer.        
    # @param dataset String - The dataset name
    def getDatasetBlocks(self, dataset):
        return  self.dbsApi.listBlockSummaries( dataset = dataset )

    ## Function to get a summary for a dataset
    # @param self: The object pointer.        
    # @param dataset String - The dataset name        
    def getDatasetSummary(self, dataset):
        datasetBlocks = self.getDatasetBlocks(dataset)
        datasetSummary = {}
        
        datasetSummary.update({"numEvents":sum( [ block['num_event'] for block in datasetBlocks ] )} )
        datasetSummary.update({"numFiles":sum( [ block['num_file'] for block in datasetBlocks ] )} )
        datasetSummary.update({"totalFileSize":sum( [ block['file_size'] for block in datasetBlocks ] ) })
        return datasetSummary
        
