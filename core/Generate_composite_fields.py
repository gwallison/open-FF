# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 16:52:03 2020

@author: Gary

Creates fields that are composites of other fields

"""

import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
#import random
from sklearn.cluster import DBSCAN
from geopy.distance import geodesic
import requests
#import IPython.display as Disp
import core.Construct_set as const_set
#import core.get_google_map as gmap


class Gen_composite_fields():
    def __init__(self,tab_manager = None):  
        self.tab_man = tab_manager
    
    def make_infServiceCo(self):
        """makes the inferred Service Company field.  passed df is an
        allrec table and needs bgSupplier and UploadKey.  Will return
        a table with UploadKey and 'infServiceCo' to be merged into an
        event table"""
        notServ = ['_empty_','_empty_entry_','unrecorded supplier',
                   'Listed Above','not a company','not assigned',
                   'operator','company supplied','ambiguous',
                   '3rd party','multiple suppliers','third party']

        df = self.tab_man.tables['allrec'].get_df() # fetch all
        sup = self.tab_man.tables['supplier'].get_df()[['iSupplier','bgSupplier']]
        df = pd.merge(df,sup,on='iSupplier',how='left')
        #print(df.columns)
        df = df[~df.bgSupplier.isin(notServ)]  # drop all the non-company values
        gb = df.groupby('iUploadKey')['bgSupplier'].agg(lambda x: x.value_counts().index[0])
        gb = gb.reset_index()
        gb.rename({'bgSupplier':'infServiceCo'},axis=1,inplace=True)
        #print(gb.columns)
        ev = self.tab_man.tables['event'].get_df()
        ev = pd.merge(ev,gb,on='iUploadKey',how='left')
        #print(ev.columns)
        self.tab_man.update_table_df(ev,'event')
       
####  generate geo-clusters
####    as of version 9.0, only one set of clusters is created and
####    much of the cluster stats are not used.  Needs to be refactored
####    if that stays the case, or put into use.


    def makeObj(self,lat,lon,upK,opN='?',API='?',wellname='?', date='?'):
        return {'upK':upK, 'lat':lat, 'lon':lon, 'opN':opN, 'API': API, 'wn':wellname, 'date': date}
    
    
    def clusterCompany(self,df,opName=None,padDic=None,eps=.2):
        """ clusters only fracks from given company. output added to panDic"""
        
        if opName==None:  # do all disclosures
            df_op = df.copy()
            print(f'Clustering ALL disclosures, Number of records = {len(df_op)}')
        else:
            """ will make and store clusters for a specific company"""
            df_op = df[df.bgOperatorName==opName].copy()
        coords = df_op[['bgLatitude','bgLongitude']].values
    
        kms_per_radian = 6371.0088
        epsilon = eps / kms_per_radian
    
        db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))
        #print(f'  --  {opName}, {len(df_op)} >> {num_clusters}')
        df_op['clusterID'] = cluster_labels    
    
        wpoffset = len(padDic.keys())
        wprange = range(wpoffset,wpoffset+num_clusters)
        #print(f'wprange = {wprange}')
        if padDic == None:
            padDic = {}
        for n in wprange:
            padDic[n] = {'company':opName,'inside':[],'centroid':(0,0), #,'outside_same':[],
                         'inUpK':[]} #,'minOutside_same':999, 'outside_other':[], 'minOutside_other':999}
    
        for index, row in df_op.iterrows():
            padDic[row.clusterID+wpoffset]['inside'].append(self.makeObj(lat=row.bgLatitude,lon=row.bgLongitude,upK=row.UploadKey,
                                                         opN=row.bgOperatorName,API=row.APINumber,wellname=row.WellName,
                                                         date=row.date))
            padDic[row.clusterID+wpoffset]['inUpK'].append(row.UploadKey)    
        
        for padid in wprange:
            # calc centroid of clusters
            lats = 0; lons = 0
            for i in padDic[padid]['inside']:
                lats += i['lat']; lons += i['lon']
            pdsize = len(padDic[padid]['inside'])
            centroid = (lats/pdsize,lons/pdsize)
            padDic[padid]['centroid'] = centroid  
    
        return padDic

    def make_company_clusters(self,df,eps=0.2):
        print(f'  Clustering by Operator with epsilon of {eps}')
        companies = list(df.bgOperatorName.unique())
        wpdic = {}
        for company in companies:
            wpdic = self.clusterCompany(df[df.bgOperatorName==company],company,wpdic,eps=eps)  
        print(f' -- Total Number clusters: {len(wpdic.keys())}')
        return wpdic
    

    def make_all_clusters(self,df,eps=1):
        print(f'  Clustering all disclosures with epsilon of {eps}')
        wpdic = {}
        wpdic = self.clusterCompany(df,None,wpdic,eps=eps)  
        print(f'Total Number clusters: {len(wpdic.keys())}')
        return wpdic
    
    def makeDF_from_wpdic(self,wpdic,fieldname):
        df = pd.DataFrame()
        for padid in wpdic.keys():
            t = pd.DataFrame({'UploadKey':wpdic[padid]['inUpK'],
                              fieldname:padid})
            df = pd.concat([df,t],sort=True)
        return df

    def make_geo_clusters(self):
        df_test = pd.merge(self.tab_man.tables['event'].get_df(),
                          self.tab_man.tables['operator'].get_df(),
                          on='iOperatorName',
                          how='left',validate='m:1')
        df_test = df_test[['UploadKey','APINumber','bgLatitude','bgLongitude',
                           'loc_flags','bgOperatorName',
                           'WellName','date']]
        # don't include the disclosures that are known location problems
        df_test.loc_flags.fillna('--',inplace=True)
        df_test = df_test[~(df_test.loc_flags.str.contains('O',regex=True))]
        
        wpdic = self.make_company_clusters(df_test,eps=0.2) # roughly 200m window       
        #wpdic = self.make_all_clusters(df_test,eps=0.2) # roughly 200m window       
        #paddf = self.makeDF_from_wpdic(wpdic,'clusterTest200m')    
        print('  Constructing the cluster structure; please be patient!')
        paddf = self.makeDF_from_wpdic(wpdic,'clusterID')
        paddf['clusterID'] = np.where(paddf.clusterID.isna(),
                                      'cBadLocationData',
                                      'c' + paddf.clusterID.round(0).astype('str'))
        
            
        ev = self.tab_man.tables['event'].get_df()
        ev = pd.merge(ev,paddf,on='UploadKey',how='left')
        #print(ev.columns)
        self.tab_man.update_table_df(ev,'event')
       
        
