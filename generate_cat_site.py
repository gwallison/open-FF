# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:00:34 2020

@author: Gary

This code is used to direct the process of making a static web site of
a chemical catelog.
"""
import core.Construct_set as const_set
import website_gen.web_gen as web_gen

data_date = '2020-02-11'

tab_const = const_set.Construct_set(fromScratch=False).get_full_set()

wg = web_gen.Web_gen(tab_man=tab_const,data_date=data_date)
wg.make_chem_list()
wg.make_all_catalogs()
wg.make_front_page()