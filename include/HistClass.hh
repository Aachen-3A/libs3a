/** \namespace HistClass
 *
 * \brief Functions to have easy histogram handling
 *
 * In this namespace different functions to interact with the histogram
 * map are included, to create, fill and write histograms in a convinient
 * way.
 */

#ifndef HistClass_hh
#define HistClass_hh

#include <iostream>
#include <map>
#include "TH1F.h"
#include "TH2F.h"
#include "THnSparse.h"
#include "TString.h"
#include "TNtupleD.h"
#include "TEfficiency.h"
#include "boost/format.hpp"
#include <stdarg.h>

/** To avoid compiler problems, we tell gcc to ignore any unused function error
 */

#ifdef __GNUC__
#define SUPPRESS_NOT_USED_WARN __attribute__ ((unused))
#else
#define SUPPRESS_NOT_USED_WARN
#endif

namespace HistClass {
    static std::map<std::string, TH1D * > histo; /*!< Map of a string and a TH1D histogram, for easy 1D histogram handling. */
    static std::map<std::string, TH2D * > histo2; /*!< Map of a string and a TH2D histogram, for easy 2D histogram handling. */
    static std::map<std::string, THnSparseD * > histon; /*!< Map of a string and a THnSparseD histogram, for easy nSparse handling. */
    static std::map<std::string, TNtupleD * > ttupple; /*!< Map of a string and a TNtupleD histogram, for easy Ntuple handling. */
    static std::map<std::string, TTree * > trees; /*!< Map of a string and a TTree histogram, for easy tree handling. */
    static std::map<std::string, TEficiency * > effs; /*!< Map of a string and a TEfficiency container. */

    /*! \brief Function to create a number of 1D histograms in the histo map
     *
     * \param[in] n_histos Number of histograms that should be created with different numbers
     * \param[in] name Name of the histograms that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void CreateHisto(Int_t n_histos, const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        for(int i = 0; i < n_histos; i++){
            TH1D * tmphist = new TH1D(Form("h1_%d_%s", i, name), xtitle, nbinsx, xlow, xup);
            tmphist->SetXTitle(xtitle);
            tmphist->Sumw2();
            histo[Form("h1_%d_%s", i, name)] = tmphist;
        }
    }

    /*! \brief Function to create a number of 1D histograms in the histo map for a specific particle
     *
     * \param[in] n_histos Number of histograms that should be created with different numbers
     * \param[in] name Name of the histograms that should be created
     * \param[in] particle Name of the particle for which the histograms are created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void CreateHisto(Int_t n_histos, const char* name, const char* particle, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        for(int i = 0; i < n_histos; i++){
            TH1D * tmphist = new TH1D(Form("h1_%d_%s_%s", i,particle, name), xtitle, nbinsx, xlow, xup);
            tmphist->SetXTitle(xtitle);
            tmphist->Sumw2();
            histo[Form("h1_%d_%s_%s", i,particle, name)] = tmphist;
        }
    }

    /*! \brief Function to create a 1D histogram in the histo map without the standard histogram naming convention (h1_...)
     *
     * \param[in] name Name of the histograms that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void CreateHistoUnchangedName(const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        TH1D * tmphist = new TH1D(Form("%s", name), xtitle, nbinsx, xlow, xup);
        tmphist->SetXTitle(xtitle);
        tmphist->Sumw2();
        histo[Form("%s", name)] = tmphist;
    }

    /*! \brief Function to create one 1D histograms in the histo map
     *
     * \param[in] name Name of the histogram that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN const static void CreateHisto(const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        TH1D * tmphist = new TH1D(Form("h1_%s", name), xtitle, nbinsx, xlow, xup);
        tmphist->SetXTitle(xtitle);
        tmphist->Sumw2();
        histo[Form("h1_%s", name)] = tmphist;
    }

    /*! \brief Function to create one 1D histogram in the histo map for a specific particle
     *
     * \param[in] name Name of the histograms that should be created
     * \param[in] particle Name of the particle for which the histograms are created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN const static void CreateHisto(const char* name, const char* particle, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        TH1D * tmphist = new TH1D(Form("h1_%s_%s", particle,name), xtitle, nbinsx, xlow, xup);
        tmphist->SetXTitle(xtitle);
        tmphist->Sumw2();
        histo[Form("h1_%s_%s", particle,name)] = tmphist;
    }

    /*! \brief Function to create one 1D histograms in the histo map with a boost name as input
     *
     * \param[in] name Name of the histogram that should be created (boost::basic_format)
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN const static void CreateHisto(boost::basic_format<char> name, Int_t nbinsx, Double_t xlow, Double_t xup, TString xtitle = "")
    {
        CreateHisto(str(name).c_str(), nbinsx, xlow, xup, xtitle);
    }

    /*! \brief Function to create one 2D histograms in the histo map
     *
     * \param[in] name Name of the histogram that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] nbinsy Number of bins on the y-axis
     * \param[in] ylow Lower edge of the y-axis
     * \param[in] yup Upper edge of the y-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     * \param[in] ytitle Optinal title of the y-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void CreateHisto(const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, Int_t nbinsy, Double_t ylow, Double_t yup, TString xtitle = "", TString ytitle = "")
    {
        std::string dummy = Form("h2_%s", name);
        histo2[dummy] = new TH2D(Form("h2_%s", name), Form("h2_%s", name), nbinsx, xlow, xup, nbinsy, ylow, yup);
        histo2[dummy] -> Sumw2();
        histo2[dummy] -> GetXaxis() -> SetTitle(xtitle);
        histo2[dummy] -> GetYaxis() -> SetTitle(ytitle);
    }

    /*! \brief Function to create a number of 2D histograms in the histo map
     *
     * \param[in] n_histos Number of histograms that should be created with different numbers
     * \param[in] name Name of the histogram that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] nbinsy Number of bins on the y-axis
     * \param[in] ylow Lower edge of the y-axis
     * \param[in] yup Upper edge of the y-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     * \param[in] ytitle Optinal title of the y-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void CreateHisto(Int_t n_histos, const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, Int_t nbinsy, Double_t ylow, Double_t yup, TString xtitle = "", TString ytitle = "")
    {
        for(int i = 0; i < n_histos; i++){
            TH2D * tmphist = new TH2D(Form("h2_%d_%s", i, name), Form("h2_%d_%s", i, name), nbinsx, xlow, xup, nbinsy, ylow, yup);
            tmphist->SetXTitle(xtitle);
            tmphist->SetYTitle(ytitle);
            tmphist->Sumw2();
            histo2[Form("h2_%d_%s", i, name)] = tmphist;
        }
    }

    /*! \brief Function to create one NSparse in the nSparse map
     *
     * \param[in] name Name of the NSparse that should be created
     * \param[in] dimension Number of dimensions that the NSparse should have
     * \param[in] bins Array with the number of bins for each dimension
     * \param[in] xmin Array of the lower edge of the axis for each dimension
     * \param[in] xmax Array of the upper edge of the axis for each dimension
     * \param[in] axisTitle[] Array of the axis title for each dimension
     */
    SUPPRESS_NOT_USED_WARN static void CreateNSparse(const char* name, int dimension, int* bins, double* xmin, double* xmax, string axisTitle[])
    {
        std::string dummy = Form("hn_%s", name);
        histon[dummy] = new THnSparseD(Form("hn_%s", name), Form("hn_%s", name), dimension,bins, xmin, xmax );
        histon[dummy] -> Sumw2();
        for(int i=0;i<dimension;i++){
            histon[dummy]->GetAxis(i)->SetTitle(axisTitle[i].c_str());
        }
    }

    /*! \brief Function to create one NtupleD in the NtupleD map
     *
     * \param[in] name Name of the NSparse that should be created
     * \param[in] varlist Colon sepereated list with the name of the branches that should be created
     * \param[in] bufsize Buffer size that the NtupleD should have
     */
    SUPPRESS_NOT_USED_WARN static void CreateTree(const char* name, const char* varlist, int bufsize)
    {
        std::string dummy = Form("tree_%s", name);
        ttupple[dummy] = new TNtupleD(Form("tree_%s", name),name,varlist,bufsize);
    }

    /*! \brief Function to create one Tree in the TTree map
     *
     * \param[in] m Map of the name and variable that should be matched to each branch
     * \param[in] name Name of the TTree that should be created
     */
    SUPPRESS_NOT_USED_WARN static void CreateTree( std::map< std::string , float > & m, const char * name)
    {
        trees[name] = new TTree(name,name);
        for( std::map< std::string , float >::iterator it=m.begin(); it!=m.end();it++){
                trees[name]->Branch(it->first.c_str(),&(it->second),Form("%s/F",it->first.c_str()));
        }
    }

    /*! \brief Function to create one 1D Efficiency container in the eff map
     *
     * \param[in] name Name of the Efficiency container that should be created
     * \param[in] nbinsx Number of bins on the x-axis
     * \param[in] xlow Lower edge of the x-axis
     * \param[in] xup Upper edge of the x-axis
     * \param[in] xtitle Optinal title of the x-axis (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN const static void CreateEff(const char* name, Int_t nbinsx, Double_t xlow, Double_t xup, const char* xtitle = "")
    {
        TEfficiency * tmpeff = new TEfficiency(Form("eff_%s", name), Form("%s;%s;%s",name,xtitle,"#epsilon"), nbinsx, xlow, xup);
        effs[Form("eff_%s", name)] = tmpeff;
    }

    /*! \brief Function to fill an event in a 1D histogram of the map
     *
     * This function fills one value with one weight for one event in one
     * specific histogram. The function also checks if the histogram exists
     * in the map, otherwise it will print an error message.
     * \param[in] n_histo Number of the histogram that should be filled
     * \param[in] name Name of the histogram which should be filled
     * \param[in] value Value that should be filled
     * \param[in] weight Weight of the event that should be filled
     */
    static void Fill(Int_t n_histo, const char * name, double value, double weight)
    {
        std::map<string, TH1D * >::iterator it =histo.find(Form("h1_%d_%s", n_histo, name));
        if(it!=histo.end()){
            it->second->Fill(value,weight);
        }else{
            std::cerr << "(Fill) No hist: " << Form("h1_%d_%s", n_histo, name) << " in map " << n_histo << std::endl;
        }
    }

    /*! \brief Function to fill an event in a 1D histogram of the map with a string as histo name
     *
     * \param[in] n_histo Number of the histogram that should be filled
     * \param[in] name Name of the histogram which should be filled (std string)
     * \param[in] value Value that should be filled
     * \param[in] weight Weight of the event that should be filled
     */
    SUPPRESS_NOT_USED_WARN static void Fill(Int_t n_histo, std::string name, double value, double weight)
    {
        Fill( n_histo, name.c_str(), value,weight);
    }

    /*! \brief Function to fill an event in a 1D histogram of the map without histo number
     *
     * This function fills one value with one weight for one event in one
     * specific histogram. The function also checks if the histogram exists
     * in the map, otherwise it will print an error message.
     * \param[in] name Name of the histogram which should be filled
     * \param[in] value Value that should be filled
     * \param[in] weight Weight of the event that should be filled
     */
    SUPPRESS_NOT_USED_WARN static void Fill(const char * name, double value, double weight)
    {
        std::map<string, TH1D * >::iterator it;
        if (strcmp( name, "h_counters")==0) {
                it =histo.find(Form("%s", name));
        }
        else {
                it =histo.find(Form("h1_%s", name));
        }

        if(it!=histo.end()){
            it->second->Fill(value,weight);
        }else{
            std::cerr << "(Fill) No hist: " << Form("h1_%s", name) << " in map " << std::endl;
        }
    }

    /*! \brief Function to fill an event in a 2D histogram of the map
     *
     * \param[in] name Name of the histogram which should be filled
     * \param[in] valuex x-value that should be filled
     * \param[in] valuey y-value that should be filled
     * \param[in] weight Weight of the event that should be filled
     */
    SUPPRESS_NOT_USED_WARN static void Fill(const char * name, double valuex, double valuey, double weight)
    {
        std::string dummy = Form("h2_%s", name);
        histo2[dummy]->Fill(valuex,valuey,weight);
    }

    /*! \brief Function to fill an event in a 2D histogram of the map
     *
     * This function fills one value with one weight for one event in one
     * specific histogram. The function also checks if the histogram exists
     * in the map, otherwise it will print an error message.
     * \param[in] n_histo Number of the histogram that should be filled
     * \param[in] name Name of the histogram which should be filled
     * \param[in] valuex x-value that should be filled
     * \param[in] valuey y-value that should be filled
     * \param[in] weight Weight of the event that should be filled
     */
    static void Fill(Int_t n_histo, const char * name, double valuex, double valuey, double weight)
    {
        std::map<string, TH2D * >::iterator it =histo2.find(Form("h2_%d_%s", n_histo, name));
        if(it!=histo2.end()){
            it->second->Fill(valuex,valuey,weight);
        }else{
            std::cerr << "(Fill) No hist: " << Form("h2_%d_%s", n_histo, name) << " in map " << n_histo << std::endl;
        }
    }

    /*! \brief Function to fill an event in a nSparse of the nSparse map
     *
     * This function fills one value with one event in one specific
     * nSparse. The function also checks if the nSparse exists
     * in the map, otherwise it will print an error message.
     * \param[in] name Name of the n which should be filled
     * \param[in] n 
     * \param[in] ...
     * \todo complete the function
     */
    SUPPRESS_NOT_USED_WARN static void FillSparse(const char * name, int n, ...)
    {
        std::map<string, THnSparseD * >::iterator it =histon.find(Form("hn_%s", name));
        if(it!=histon.end()){
            vector <double> v;
            va_list vl;
            va_start(vl,n);
            for (int i=0;i<n;i++){
                v.push_back(va_arg(vl,double));
            }
            va_end(vl);
            //it->second->Fill(&v[0]);
        }else{
            std::cerr << "(Fill) No hist: " << name << " in map " << std::endl;
        }
    }

    /*! \brief Function to fill an event in a NTupleD of the map
     *
     * \param[in] name Name of the NTupleD which should be filled
     * \param[in] values Array of values that should be filled
     */
    SUPPRESS_NOT_USED_WARN static void FillTree(const char * name, double* values)
    {
        std::string dummy = Form("tree_%s", name);
        ttupple[dummy]->Fill(values);
    }

    /*! \brief Function to fill an event in a TTree of the map
     *
     * \param[in] name Name of the TTree which should be filled
     */
    SUPPRESS_NOT_USED_WARN static void FillTree( const char * name )
    {
        trees[name]->Fill();
    }

    /*! \brief Function to fill an event in a efficiency container of the map
     *
     * \param[in] name Name of the histogram which should be filled
     * \param[in] valuex x-value that should be filled
     * \param[in] passed Boolean if the event passed or not
     */
    SUPPRESS_NOT_USED_WARN static void Fill(const char * name, double valuex, bool passed)
    {
        std::string dummy = Form("eff_%s", name);
        effs[dummy]->Fill(passed,valuex);
    }

    /*! \brief Function to write one 1D histogram of the map
     *
     * \param[in] n_histo Number of the histogram that should be written
     * \param[in] name Name of the histogram that should be written
     */
    SUPPRESS_NOT_USED_WARN static void Write(Int_t n_histo, const char * name)
    {
        std::string dummy = Form("h1_%d_%s", n_histo, name);
        histo[dummy]->Write();
    }

    /*! \brief Function to write one 1D histogram of the map without number
     *
     * \param[in] name Name of the histogram that should be written
     */
    SUPPRESS_NOT_USED_WARN static void Write(const char * name)
    {
        std::string dummy = Form("h1_%s", name);
        histo[dummy]->Write();
    }

    /*! \brief Function to write many 1D histograms of the map
     *
     * This function writes all histograms of the map with the
     * default options, otherwise it writes all histograms that
     * contain the given string in there name.
     * \param[in] name Optional string that all histogram names that should be written contain (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll(const char * name = "")
    {
        std::map<std::string, TH1D * >::iterator it;
        for (std::map<std::string, TH1D * >::iterator it=histo.begin(); it!=histo.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
    }

    /*! \brief Function to write many 2D histograms of the map
     *
     * This function writes all histograms of the map with the
     * default options, otherwise it writes all histograms that
     * contain the given string in there name.
     * \param[in] name Optional string that all histogram names that should be written contain (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll2(const char * name = "")
    {
        std::map<std::string, TH2D * >::iterator it;
        for (std::map<std::string, TH2D * >::iterator it=histo2.begin(); it!=histo2.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
    }

    /*! Example to create a nice folder structure in your output folder
     *   //void specialAna::channel_writer(TFile* file, const char* channel) {
     *       //file1->cd();
     *       //file1->mkdir(channel);
     *       //for ( int i = 0; i < channel_stages[channel]; i++) {
     *           //char n_satge = (char)(((int)'0')+i);
     *           //file1->mkdir(TString::Format("%s/Stage_%c", channel, n_satge));
     *           //file1->cd(TString::Format("%s/Stage_%c/", channel, n_satge));
     *           //HistClass::WriteAll(TString::Format("_%s_", channel),TString::Format("%s:_%c_", channel, n_satge),TString::Format("sys"));
     *           //file1->cd();
     *           //file1->mkdir(TString::Format("%s/Stage_%c/sys", channel, n_satge));
     *           //file1->cd(TString::Format("%s/Stage_%c/sys/", channel, n_satge));
     *           //HistClass::WriteAll(TString::Format("_%s_", channel),TString::Format("_%c_:sys", n_satge));
     *       //}
     *       //file1->cd();
     *   //}
    */

    /*! \brief Function split a string at a delimiter
     *
     * This function splits a given string at a given delimineter,
     * and pushes the results in a given vector.
     * \param[in] &s String that should be split
     * \param[in] delim Delimiter where the string should be split
     * \param[in] &elems Vector in which the substrings should be pushed
    */
    SUPPRESS_NOT_USED_WARN void split(const std::string &s, char delim, std::vector<std::string> &elems) {
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, delim)) {
            elems.push_back(item);
        }
    }

    /*! \brief Function split a string at a delimiter and return the results
     *
     * This function splits a given string at a given delimineter,
     * and returns the resulting substrings as a vector.
     * \param[in] &s String that should be split
     * \param[in] delim Delimiter where the string should be split
     * \return[out] elems Vector in which the substrings were pushed
    */
    SUPPRESS_NOT_USED_WARN std::vector<std::string> split(const std::string &s, char delim) {
        std::vector<std::string> elems;
        split(s, delim, elems);
        return elems;
    }

    /*! \brief Function to write many 1D histograms which contain specific strings of the map
     *
     * This function writes all histograms of the map with that
     * contain the given string in there name. The written histo-
     * grams also have to contain a list of strings that are sepe-
     * rated by a ':'.
     * \param[in] name String that all histogram names that should be written contain
     * \param[in] contains_i String that of names (seperated by ':') that the histogram name should contain
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll(const char * name, const char * contains_i)
    {
        const std::string contains(contains_i);
        std::vector<std::string> i_cont = split(contains,':');
        std::map<std::string, TH1D * >::iterator it;
        for (std::map<std::string, TH1D * >::iterator it=histo.begin(); it!=histo.end(); ++it){
            if(std::string::npos!=it->first.find(name)){
                bool do_write = false;
                for (uint i = 0; i < i_cont.size(); i++) {
                    if (std::string::npos!=it->first.find(i_cont[i])){
                        do_write = true;
                    }else{
                        do_write = false;
                        break;
                    }
                }
                if ( do_write ) {
                    it->second -> Write();
                }
            }
        }
    }

    /*! \brief Function to write many 2D histograms which contain specific strings of the map
     *
     * This function writes all histograms of the map with that
     * contain the given string in there name. The written histo-
     * grams also have to contain a list of strings that are sepe-
     * rated by a ':'.
     * \param[in] name String that all histogram names that should be written contain
     * \param[in] contains_i String that of names (seperated by ':') that the histogram name should contain
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll2(const char * name, const char * contains_i)
    {
        const std::string contains(contains_i);
        std::vector<std::string> i_cont = split(contains,':');
        std::map<std::string, TH2D * >::iterator it;
        for (std::map<std::string, TH2D * >::iterator it=histo2.begin(); it!=histo2.end(); ++it){
            if(std::string::npos!=it->first.find(name)){
                bool do_write = false;
                for (uint i = 0; i < i_cont.size(); i++) {
                    if (std::string::npos!=it->first.find(i_cont[i])){
                        do_write = true;
                    }else{
                        do_write = false;
                        break;
                    }
                }
                if ( do_write ) {
                    it->second -> Write();
                }
            }
        }
    }

    /*! \brief Function to write many 1D histograms which (not) contain specific strings of the map
     *
     * This function writes all histograms of the map with that
     * contain the given string in there name. The written histo-
     * grams also have to contain a list of strings that are sepe-
     * rated by a ':'. In this version also a list of strings that
     * should not be contained in the histogram name can be given.
     * \param[in] name String that all histogram names that should be written contain
     * \param[in] contains_i String that of names (seperated by ':') that the histogram name should contain
     * \param[in] vetos_i String that of names (seperated by ':') that the histogram name should not contain
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll(const char * name, const char * contains_i, const char * vetos_i)
    {
        const std::string contains(contains_i);
        const std::string vetos(vetos_i);
        std::vector<std::string> i_cont = split(contains,':');
        std::vector<std::string> i_veto = split(vetos,':');
        std::map<std::string, TH1D * >::iterator it;
        for (std::map<std::string, TH1D * >::iterator it=histo.begin(); it!=histo.end(); ++it){
            if(std::string::npos!=it->first.find(name)){
                bool do_write = false;
                for (uint i = 0; i < i_cont.size(); i++) {
                    if (std::string::npos!=it->first.find(i_cont[i])){
                        do_write = true;
                    }else{
                        do_write = false;
                        break;
                    }
                }
                for (uint i = 0; i < i_veto.size(); i++) {
                    if (std::string::npos!=it->first.find(i_veto[i])){
                        do_write = false;
                        break;
                    }
                }
                if ( do_write ) {
                    it->second -> Write();
                }
            }
        }
    }

    /*! \brief Function to write many 2D histograms which (not) contain specific strings of the map
     *
     * This function writes all histograms of the map with that
     * contain the given string in there name. The written histo-
     * grams also have to contain a list of strings that are sepe-
     * rated by a ':'. In this version also a list of strings that
     * should not be contained in the histogram name can be given.
     * \param[in] name String that all histogram names that should be written contain
     * \param[in] contains_i String that of names (seperated by ':') that the histogram name should contain
     * \param[in] vetos_i String that of names (seperated by ':') that the histogram name should not contain
     */
    SUPPRESS_NOT_USED_WARN static void WriteAll2(const char * name, const char * contains_i, const char * vetos_i)
    {
        const std::string contains(contains_i);
        const std::string vetos(vetos_i);
        std::vector<std::string> i_cont = split(contains,':');
        std::vector<std::string> i_veto = split(vetos,':');
        std::map<std::string, TH2D * >::iterator it;
        for (std::map<std::string, TH2D * >::iterator it=histo2.begin(); it!=histo2.end(); ++it){
            if(std::string::npos!=it->first.find(name)){
                bool do_write = false;
                for (uint i = 0; i < i_cont.size(); i++) {
                    if (std::string::npos!=it->first.find(i_cont[i])){
                        do_write = true;
                    }else{
                        do_write = false;
                        break;
                    }
                }
                for (uint i = 0; i < i_veto.size(); i++) {
                    if (std::string::npos!=it->first.find(i_veto[i])){
                        do_write = false;
                        break;
                    }
                }
                if ( do_write ) {
                    it->second -> Write();
                }
            }
        }
    }

    /*! \brief Function to write many TTrees and TNtupleDs of the maps
     *
     * This function writes all TTrees and TNtupleDs of the maps
     * with the default options, otherwise it writes all histograms
     * that contain the given string in there name.
     * \param[in] name Optional string that all TTrees or TNtupleDs names that should be written contain (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void WriteAllTrees(const char * name = "")
    {
        for (std::map<std::string, TNtupleD * >::iterator it=ttupple.begin(); it!=ttupple.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
        for (std::map<std::string, TTree * >::iterator it=trees.begin(); it!=trees.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
    }

    /*! \brief Function to write many TNsparses of the map
     *
     * This function writes all nSparses of the map with the
     * default options, otherwise it writes all histograms that
     * contain the given string in there name.
     * \param[in] name Optional string that all nSparses names that should be written contain (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void WriteN(const char * name = "")
    {
        std::map<std::string, THnSparseD * >::iterator it;
        for (std::map<std::string, THnSparseD * >::iterator it=histon.begin(); it!=histon.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
    }

    /*! \brief Function to write one 2D histogram of the map
     *
     * \param[in] name Name of the histogram that should be written
     */
    SUPPRESS_NOT_USED_WARN static void Write2(const char * name)
    {
        std::string dummy = Form("h2_%s", name);
        histo2[dummy]->Write();
    }

    /*! \brief Function to write many efficiency containers of the map
     *
     * This function writes all efficiency containers of the map with the
     * default options, otherwise it writes all efficiency containers that
     * contain the given string in there name.
     * \param[in] name Optional string that all efficiency containers names that should be written contain (DEFAULT = "")
     */
    SUPPRESS_NOT_USED_WARN static void WriteAllEff(const char * name = "")
    {
        std::map<std::string, TEfficiency * >::iterator it;
        for (std::map<std::string, TEfficiency * >::iterator it=effs.begin(); it!=effs.end(); ++it){
            if(strcmp( name, "") != 0 && std::string::npos!=it->first.find(name)){
                it->second -> Write();
            }else if(strcmp( name, "") == 0){
                it->second -> Write();
            }
        }
    }

    /*! \brief Function to set all negative bin contents to zero for a 1D histogram
     *
     * \param[in] n_histo Number of the histogram that should be modified
     * \param[in] name Name of the histogram that should be modified
     */
    SUPPRESS_NOT_USED_WARN static void SetToZero(Int_t n_histo, const char * name)
    {
        std::string dummy = Form("h1_%d_%s", n_histo, name);
        int Nbins2 = histo[dummy] -> GetNbinsX();
        for ( int bb = 0; bb < Nbins2+1; bb++) {
            double binValue = histo[dummy] -> GetBinContent(bb);
            if (binValue < 0) {
                 histo[dummy] -> SetBinContent(bb,0.);
            }
        }
    }

    /*! \brief Function to get one 1D histogram from the map without number
     *
     * \param[in] name Name of the histogram that should be returned
     * \returns histo Returned histogram
     */
    SUPPRESS_NOT_USED_WARN static TH1D* ReturnHist(const char * name)
    {
        std::string dummy="";
        if (strcmp( name, "h_counters")==0) {
            dummy = Form("%s", name);
        } else {
            dummy = Form("h1_%s", name);
        }
        return histo[dummy];
    }

    /*! \brief Function to give one 1D histogram from the map alphanumeric bin labels without number
     *
     * \param[in] name Name of the histogram that should get bin names
     * \param[in] n_bins of bins that should be renamed
     * \param[in] d_mydisc Array with the names that th bins should get
     */
    SUPPRESS_NOT_USED_WARN static void NameBins(const char * name, const uint n_bins, TString* d_mydisc)
    {
        std::string dummy = Form("h1_%s", name);
        TString mydisc[n_bins];
        for(uint i = 0; i < n_bins; i++) mydisc[i] = d_mydisc[i];
        for(uint i = 0; i < n_bins; i++) {
            histo[dummy]->GetXaxis()->SetBinLabel(i+1,mydisc[i]);
        }
    }

    /*! \brief Function to give one 1D histogram from the map alphanumeric bin labels
     *
     * \param[in] n_histo Number of the histogram that should get bin names
     * \param[in] name Name of the histogram that should get bin names
     * \param[in] n_bins of bins that should be renamed
     * \param[in] d_mydisc Array with the names that th bins should get
     */
    SUPPRESS_NOT_USED_WARN static void NameBins(Int_t n_histo, const char * name, const uint n_bins, TString* d_mydisc)
    {
        for(int i = 0; i < n_histo; i++){
            std::string dummy = Form("h1_%d_%s", i, name);
            TString mydisc[n_bins];
            for(uint i = 0; i < n_bins; i++) mydisc[i] = d_mydisc[i];
            for(uint i = 0; i < n_bins; i++) {
                histo[dummy]->GetXaxis()->SetBinLabel(i+1,mydisc[i]);
            }
        }
    }

}
#endif
