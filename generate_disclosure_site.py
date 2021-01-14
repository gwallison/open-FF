# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:00:34 2020

@author: Gary

This code is used to direct the process of making a static web site of
a disclosure catelog.
"""
import core.Construct_set as const_set
#import website_gen.web_gen as web_gen
import website_gen.API_web_gen as API_gen

data_date = '2020-10-23'

tab_const = const_set.Construct_set(fromScratch=False).get_full_set()

ag = API_gen.API_web_gen(tab_man=tab_const,data_date=data_date)
ag.make_cluster_set()

ag.make_api_set()
#ag.make_index_page()
#apidf = ag.getAPIdf()