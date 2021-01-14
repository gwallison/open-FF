# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:05:07 2020

@author: Gary
"""
import numpy as np
import pandas as pd
import shutil
import os
import subprocess
from datetime import datetime
import pickle
import core.get_google_map as ggmap

today = datetime.today()

# for nicer displays of numbers: round to significant figures.
from math import log10, floor
def round_sig(x, sig=2):
    try:
        if abs(x)>=1:
            out =  int(round(x, sig-int(floor(log10(abs(x))))-1))
            return f"{out:,d}" # does the right thing with commas
        else: # fractional numbers
            return str(round(x, sig-int(floor(log10(abs(x))))-1))
    except:
        return x
    

# =============================================================================
# def round_sig(x, sig=2):
#     try:
#         return int(round(x, sig-int(floor(log10(abs(x))))-1))
#     except:
#         return x
# 
# =============================================================================
class API_web_gen():
    def __init__(self,tab_man=None,data_date='UNKNOWN'):
        self.tab_man = tab_man
        self.data_date = data_date
        self.outdir = './tmp/disclosures/'
        self.css_fn = './website_gen/style.css'
        self.initialize_dir(self.outdir)
        self.filtered_fields = ['reckey', 'PercentHFJob', 'record_flags', 
                                'bgMass', 'UploadKey', 'OperatorName',
                                'bgOperatorName',
                                'APINumber', 'TotalBaseWaterVolume',
                                'TotalBaseNonWaterVolume', 'FFVersion', 
                                'TVD', 'StateName', 'StateNumber', 'CountyName', 
                                'CountyNumber', 'TradeName',
                                'Latitude', 'Longitude', 'Projection',
                                'data_source', 'bgStateName', 'bgCountyName', 
                                'bgLatitude', 'bgLongitude', 'date',
                                'IngredientName', 'Supplier', 'bgSupplier', 
                                'Purpose', 'CASNumber', 'bgCAS','infServiceCo',
                                'bgIngredientName', 'proprietary', 
                                'eh_Class_L1', 'eh_Class_L2', 'eh_CAS', 
                                'eh_IngredientName', 'eh_subs_class', 
                                'eh_function','is_on_TEDX']
# =============================================================================
#         self.allrec = self.tab_man.get_df_cas(keepcodes='',
#                                          removecodes='',
#                                          event_fields=[])
#         self.allrec = self.allrec.filter(self.filtered_fields,axis=1)
#         self.allrec['TradeName_trunc'] = np.where(self.allrec.TradeName.str.len()>30,
#                                                   self.allrec.TradeName.str[:30]+'...',
#                                                   self.allrec.TradeName)
# 
# =============================================================================

        self.locdf = self.tab_man.get_df_location()
        self.locdf['clusterID'] = np.where(self.locdf.clusterID.isna(),
                                           'cBadLocationData',
                                           self.locdf.clusterID)
        self.locdf = pd.merge(self.locdf,self.tab_man.tables['operator'].get_df(),
                              on='iOperatorName',how='left')
        print(f'Len of location df: {len(self.locdf)}')
        print(f'Num of APIs: {len(self.locdf.APINumber.unique())}')
        self.locdf['api10'] = self.locdf.APINumber.str[:10]
        print(f'Num of API-10s: {len(self.locdf.api10.unique())}')
        self.apilst = list(self.locdf.api10.unique())
        self.clus_set = list(self.locdf.clusterID.unique())
        #self.makeAPIdict()
        #print(self.locdf.columns)

        
    def makeAPIdict(self):
        print('-- making API dictionary')
        self.apiDict = {}
        for i,row in self.locdf.iterrows():
            if row.api10 not in self.apiDict:
                self.apiDict[row.api10] = {'state':row.bgStateName,
                                           'county':row.bgCountyName,
                                           'operator':row.bgOperatorName,
                                           'date': str(row.date).split(' ')[0],
                                           'num_disclosures': 1,
                                           'data_source':row.data_source}
            else:
                try:
                    if row.bgStateName not in self.apiDict[row.api10]['state']:
                        self.apiDict[row.api10]['state'] += '<br>'+row.bgStateName
                    if row.bgCountyName not in self.apiDict[row.api10]['county']:
                        self.apiDict[row.api10]['county'] += '<br>'+row.bgCountyName
                    if row.bgOperatorName not in self.apiDict[row.api10]['operator']:
                        self.apiDict[row.api10]['operator'] += '<br>'+row.bgOperatorName
                    if row.data_source not in self.apiDict[row.api10]['data_source']:
                        self.apiDict[row.api10]['data_source'] += '<br>'+row.data_source
                    tdate = str(row.date).split(' ')[0]
                    if tdate not in self.apiDict[row.api10]['date']:
                        self.apiDict[row.api10]['date'] += '<br>'+tdate
                    self.apiDict[row.api10]['num_disclosures'] += 1
                except:
                   print(f'{row.api10}: {self.apiDict[row.api10]}')

    def initialize_dir(self,dir):
        print('Removing previous files')
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
        shutil.copyfile(self.css_fn,self.outdir+'style.css')
        

    def compile_page(self,title='empty title',header='',body=''):
        return f"""<!DOCTYPE html>
<html>
  <head>
    <title>{title}</title>
    <link rel='stylesheet' href='style.css' />
  </head>
  <body>
      <h1>{header}</h1>
      <h4>Data cleaned and extracted from <a href=https://www.FracFocus.org >FracFocus</a>, downloaded on {self.data_date}</h4>
    {body}
  </body>
</html>
"""

    def save_page(self,webtxt='', fn='index.html'):
        with open(self.outdir+fn,'w') as f:
            f.write(webtxt)

    def add_table_line(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <td> {item} </td> \n'
        s+= '   </tr>\n'
        return s
    
    def add_table_head(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <th> {item} </th> \n'
        s+= '   </tr>\n'
        return s

    def getLinkToClusMap(self,df,clusterID):
        return ''

    def getLinkToClusPage(self,clusterID):
        return ''
    
    def make_cluster_set(self):
        print('Starting Cluster assembly...')
        for i,clus in enumerate(self.clus_set):
            #print(f'{i}, {clus}')
            df = self.locdf[self.locdf.clusterID==clus].sort_values(['api10','date'])
            #print(f'{i}, {clus}, {len(df)}')
            metahead = ['API','end<br>date',
                        'source','operator',
                        'inf service co',
                        'Base water<br>volume (gal)',
                        'TVD','lat','lon','google map',
                        'clusterID','pos flags','neg flags',
                        'date added']  
            t = '<br><br><table>\n'
            t += self.add_table_head(metahead)
            currapi = ''
            locations = []
            for idx, row in df.iterrows():
                if currapi == row.api10:
                    apilink = ''
                else:
                    apilink = f'<a href="{row.api10}.html">{row.api10}</a>'
                    currapi = row.api10
                    locations.append((row.bgLatitude,row.bgLongitude))
                l = ggmap.getSearchLink(lat=row.bgLatitude,lon=row.bgLongitude)
                l = ggmap.wrap_URL_in_html(l,'SearchMap')
                lst = [apilink,str(row.date).split(' ')[0],
                       row.data_source,row.bgOperatorName,row.infServiceCo,
                       row.TotalBaseWaterVolume,row.TVD,
                       row.bgLatitude,row.bgLongitude,l,
                       row.clusterID,
                       #str(int(row.clusterTest200m)),
                       row.ev_flag_pos, row.ev_flag_neg,
                       str(row.date_added).split(' ')[0]]
                t += self.add_table_line(lst)
            t += '</table>\n'
            t = ggmap.wrap_URL_in_html(ggmap.getURL(locations),
                                       'StaticMap of Cluster APIs') +'<br>' + t
            s = self.compile_page(title=clus,#row.api10,
                                  header=f'Cluster Number: {clus}<br>  -- state: {row.bgStateName}<br>  -- county: {row.bgCountyName}', 
                                  body=t )
            self.save_page(s,clus+'.html') #row.api10+'.html')
            if i%1000==0:
                print(f'Clusters processed: {i}')
            #if i==300:
            #    break

    def make_api_set(self):
        #print(self.locdf.columns)
        for i,api in enumerate(self.apilst):
            df = self.locdf[self.locdf.api10==api].sort_values('date')
            #print(f'{api}: Len: {len(df)}')
            locations = []
            for idx, row in df.iterrows():
                locations.append((row.bgLatitude,row.bgLongitude))
            cstr = ggmap.wrap_URL_in_html(ggmap.getURL(locations),
                                       'StaticMap of Disclosures APIs') +'<br>' 
            cstr += '<h2> Clustsers </h2>\n'    
            #print(f'{cstr}, {len(self.clus_set)}, {self.clus_set[:5]}')
            for c in list(df.clusterID.unique()):
                tmp = list(self.locdf[self.locdf.clusterID==c].api10.unique())
                cstr += f'{ggmap.wrap_URL_in_html(c+".html",c)}: {tmp}<br>\n'
            metahead = ['API suffix','data','end<br>date',
                        'source','operator',
                        'inf service co',
                        'Base water<br>volume (gal)',
                        'TVD','lat','lon','google map',
                        'clusterID','pos flags','neg flags',
                        'date added']  
            t = '<br><br><table>\n'
            t += self.add_table_head(metahead)
            #dstr = '<br><h1>Data from individual disclosures</h1>'
            for idx, row in df.iterrows():
                dlink = f'<a href="#{row.UploadKey}">data</a>'
                l = ggmap.getSearchLink(lat=row.bgLatitude,lon=row.bgLongitude)
                l = ggmap.wrap_URL_in_html(l,'Map')
                lst = [row.APINumber[10:],dlink,str(row.date).split(' ')[0],
                       row.data_source,row.bgOperatorName,row.infServiceCo,
                       row.TotalBaseWaterVolume,row.TVD,
                       row.bgLatitude,row.bgLongitude,l,
                       row.clusterID,
                       #str(int(row.clusterTest200m)),
                       row.ev_flag_pos, row.ev_flag_neg,
                       str(row.date_added).split(' ')[0]]
                t += self.add_table_line(lst)
            
# =============================================================================
#### INCLUDING DATA TAKES ABOUT 40 HOURS TO COMPILE!!!
#                 # make data table in dstr for this disclosure
#                 dstr += f'<a name="{row.UploadKey}"></a>'
#                 dstr += f'<h2>{row.UploadKey} </h2>\n'
#                 dstr += '<br><br><table>\n'
#                 dstr += self.add_table_head(metahead)
#                 dstr += self.add_table_line(lst)
#                 dstr += '</table>\n'
#                 # now get data
#                 sm = self.allrec[self.allrec.UploadKey==row.UploadKey].copy()
#                 sm = sm.sort_values(['TradeName_trunc','bgCAS'])
#                 print(f' -- {row.APINumber[10:]} {str(row.date).split(" ")[0]} {row.data_source}; {len(sm)} records')
#                 dstr += '<br><table>\n'
#                 dstr += self.add_table_head(['bgSupplier','TradeName_tru',
#                                              'Proprietary','name',
#                                              'bgCAS','PercentHFJob','bgMass',
#                                              'record_flags'])
#                 for sidx,sm_row in sm.iterrows():
#                     lst = [sm_row.bgSupplier,sm_row.TradeName_trunc,
#                            sm_row.proprietary,sm_row.bgIngredientName,
#                            sm_row.bgCAS,
#                            #round(sm_row.PercentHFJob,4),
#                            str(round_sig(sm_row.PercentHFJob,4)),
#                            str(round_sig(sm_row.bgMass,sig=3)),
#                            #sm_row.bgMass,
#                            sm_row.record_flags]
#                     dstr += self.add_table_line(lst)
#                 dstr += '</table>\n'
#                     
# =============================================================================
            t += '</table>\n'

            t += cstr #+ dstr
            s = self.compile_page(title=api,#row.api10,
                                  header=f'API Number: {api}<br>  -- state: {row.bgStateName}<br>  -- county: {row.bgCountyName}', 
                                  body=t )
            self.save_page(s,api+'.html') #row.api10+'.html')
            if i%1000==0:
                print(f'API10s processed: {i}')
            #if i==1000:
            #    break


    def make_index_page(self):
        t = '<table>\n'
        t += self.add_table_head(['APINumber \n (10 digits)','State',
                                  'County','Operator','date source',
                                  'date','num_disclosures'])
        for api in list(self.apiDict.keys()): #[:300]:
            t += self.add_table_line([api,
                                      self.apiDict[api]['state'],
                                      self.apiDict[api]['county'],
                                      self.apiDict[api]['operator'],
                                      self.apiDict[api]['data_source'],
                                      self.apiDict[api]['date'],
                                      self.apiDict[api]['num_disclosures']])
        t+= '</table>'

        s = self.compile_page('API index','Disclosure index,<br> -- by API Number',
                              body = t )
        self.save_page(s)
        
    def getAPIdf(self):
        return pd.DataFrame(self.apiDict)

